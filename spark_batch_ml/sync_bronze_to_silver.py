import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

TS_HOST = os.getenv("TIMESCALE_HOST", "worker")
TS_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TS_DB = os.getenv("TIMESCALE_DB", "eonet_db")
TS_USER = os.getenv("TIMESCALE_USER", "eonet_user")
TS_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "eonet_pass")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("NASA-EONET-Sync-Bronze-To-Silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read all Bronze Parquet records and deduplicate by record_id
    bronze_df = spark.read.parquet("hdfs://master:9000/datalake/raw/eonet")
    silver_df = (
        bronze_df
        .filter(F.col("record_id").isNotNull() & F.col("event_time_utc").isNotNull())
        .dropDuplicates(["record_id"])
        .withColumn("event_date", F.to_date("event_time_utc"))
    )

    # 2. Write complete deduplicated Silver Parquet table to HDFS
    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("category_id", "event_date")
        .parquet("hdfs://master:9000/datalake/cleaned/eonet")
    )

    # 3. Upsert all deduplicated records into TimescaleDB on worker
    rows = [
        (
            r.record_id, r.event_id, r.title, r.category_id, r.source_id,
            r.status, r.event_time_utc, r.first_seen_utc, r.closed_time_utc,
            r.ingested_at_utc, r.longitude, r.latitude, r.magnitude_value,
            r.magnitude_unit, r.temp_2m_c, r.humidity_2m_pct,
            r.wind_10m_ms, r.precip_mm_day
        )
        for r in silver_df.collect()
    ]

    sql = """
        INSERT INTO eonet_events_stream (
            record_id, event_id, title, category_id, source_id, status,
            event_time_utc, first_seen_utc, closed_time_utc, ingested_at_utc,
            longitude, latitude, magnitude_value, magnitude_unit,
            temp_2m_c, humidity_2m_pct, wind_10m_ms, precip_mm_day
        ) VALUES %s
        ON CONFLICT (record_id, event_time_utc) DO UPDATE SET
            status = EXCLUDED.status,
            closed_time_utc = EXCLUDED.closed_time_utc,
            ingested_at_utc = EXCLUDED.ingested_at_utc,
            temp_2m_c = EXCLUDED.temp_2m_c,
            humidity_2m_pct = EXCLUDED.humidity_2m_pct,
            wind_10m_ms = EXCLUDED.wind_10m_ms,
            precip_mm_day = EXCLUDED.precip_mm_day;
    """

    conn = psycopg2.connect(
        host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
        print(f"Synced {len(rows)} deduplicated records into Silver Parquet & TimescaleDB.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
