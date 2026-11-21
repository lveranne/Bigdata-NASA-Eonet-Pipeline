import os
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

load_dotenv()

TS_HOST = os.getenv("TIMESCALE_HOST", "worker")
TS_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TS_DB = os.getenv("TIMESCALE_DB", "eonet_db")
TS_USER = os.getenv("TIMESCALE_USER", "eonet_user")
TS_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "eonet_pass")


def upsert_predictions(rows: list) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO eonet_predictions (
            event_id, category_id, status,
            predicted_duration_hr, predicted_severity, severity_label, scored_at_utc
        ) VALUES %s
        ON CONFLICT (event_id) DO NOTHING;
    """
    conn = psycopg2.connect(
        host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
        print(f"Upserted {len(rows)} event predictions (open + closed) into TimescaleDB.")
    finally:
        conn.close()


import subprocess

def _list_gz_parquet_files(hdfs_dir: str) -> list[str]:
    proc = subprocess.run(
        ["hdfs", "dfs", "-ls", "-R", hdfs_dir],
        capture_output=True,
        text=True,
        check=False,
    )
    files = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if parts and parts[-1].endswith(".gz.parquet"):
            p = parts[-1]
            if not p.startswith("hdfs://"):
                p = f"hdfs://master:9000{p}"
            files.append(p)
    return files


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("NASA-EONET-Score-Live-Stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load("hdfs://master:9000/models/eonet/duration_pipeline")
    df_hist = spark.read.parquet("hdfs://master:9000/datalake/silver/eonet")
    gz_files = _list_gz_parquet_files("/datalake/cleaned/eonet")
    if gz_files:
        df_live = spark.read.option("mergeSchema", "true").parquet(*gz_files)
        for col_name, field in [(f.name, f.dataType) for f in df_hist.schema.fields]:
            if col_name in df_live.columns and str(df_live.schema[col_name].dataType) != str(field):
                df_live = df_live.withColumn(col_name, F.col(col_name).cast(field))
        df = df_hist.unionByName(df_live, allowMissingColumns=True)
    else:
        df = df_hist
    events = (
        df.groupBy("event_id", "category_id")
        .agg(
            F.last("status", ignorenulls=True).alias("status"),
            F.min("first_seen_utc").alias("start_utc"),
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
            F.max("magnitude_value").alias("magnitude_value"),
            F.avg("temp_2m_c").alias("temp_2m_c"),
            F.avg("humidity_2m_pct").alias("humidity_2m_pct"),
            F.avg("wind_10m_ms").alias("wind_10m_ms"),
            F.avg("precip_mm_day").alias("precip_mm_day"),
        )
        .withColumn("start_month", F.month("start_utc").cast("double"))
        .withColumn("abs_latitude", F.abs(F.col("latitude")))
        .withColumn("has_magnitude", F.when(F.col("magnitude_value").isNotNull(), 1.0).otherwise(0.0))
        .fillna({
            "latitude": 0.0,
            "longitude": 0.0,
            "abs_latitude": 0.0,
            "start_month": 6.0,
            "magnitude_value": 0.0,
            "temp_2m_c": 15.0,
            "humidity_2m_pct": 65.0,
            "wind_10m_ms": 5.0,
            "precip_mm_day": 0.0,
        })
    )

    scored = (
        model.transform(events)
        .withColumn("predicted_duration_hr", F.greatest(F.lit(1.0), F.round(F.col("prediction"), 2)))
        .withColumn(
            "predicted_severity",
            F.when(F.col("predicted_duration_hr") < 48.0, F.lit(0))
            .when(F.col("predicted_duration_hr") <= 168.0, F.lit(1))
            .otherwise(F.lit(2)),
        )
        .withColumn(
            "severity_label",
            F.when(F.col("predicted_severity") == 0, F.lit("Low"))
            .when(F.col("predicted_severity") == 1, F.lit("Moderate"))
            .otherwise(F.lit("High")),
        )
        .withColumn("scored_at_utc", F.lit(datetime.now(timezone.utc)))
    )

    # Save Gold curated feature + prediction table to HDFS
    scored.write.mode("overwrite").parquet("hdfs://master:9000/datalake/curated/eonet_features")

    # Upsert into TimescaleDB on worker
    db_rows = [
        (
            r.event_id, r.category_id, r.status,
            float(r.predicted_duration_hr), int(r.predicted_severity),
            r.severity_label, r.scored_at_utc
        )
        for r in scored.select(
            "event_id", "category_id", "status",
            "predicted_duration_hr", "predicted_severity", "severity_label", "scored_at_utc"
        ).collect()
    ]
    upsert_predictions(db_rows)


if __name__ == "__main__":
    main()
