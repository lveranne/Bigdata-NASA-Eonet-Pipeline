import os
import json
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nasa-eonet-raw")
TS_HOST = os.getenv("TIMESCALE_HOST", "worker")
TS_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TS_DB = os.getenv("TIMESCALE_DB", "eonet_db")
TS_USER = os.getenv("TIMESCALE_USER", "eonet_user")
TS_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "eonet_pass")

EVENT_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("record_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("source_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time_utc", TimestampType(), True),
    StructField("first_seen_utc", TimestampType(), True),
    StructField("closed_time_utc", TimestampType(), True),
    StructField("ingested_at_utc", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("magnitude_value", DoubleType(), True),
    StructField("magnitude_unit", StringType(), True),
    StructField("temp_2m_c", DoubleType(), True),
    StructField("humidity_2m_pct", DoubleType(), True),
    StructField("wind_10m_ms", DoubleType(), True),
    StructField("precip_mm_day", DoubleType(), True),
])


class TimescaleMetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        try:
            p = json.loads(event.progress.json)
            now_utc = datetime.now(timezone.utc)
            batch_id = int(p.get("batchId", 0))
            num_rows = int(p.get("numInputRows", 0))
            in_rps = float(p.get("inputRowsPerSecond", 0.0) or 0.0)
            proc_rps = float(p.get("processedRowsPerSecond", 0.0) or 0.0)
            dur_ms = int(p.get("durationMs", {}).get("triggerExecution", 0))

            wm_str = p.get("eventTime", {}).get("watermark")
            wm_delay = 0.0
            if wm_str:
                wm_dt = datetime.fromisoformat(wm_str.replace("Z", "+00:00"))
                wm_delay = max(0.0, (now_utc - wm_dt).total_seconds())

            conn = psycopg2.connect(
                host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO eonet_stream_metrics (
                        batch_time_utc, batch_id, num_input_rows,
                        input_rows_per_sec, processed_rows_per_sec,
                        batch_duration_ms, watermark_delay_sec
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (now_utc, batch_id, num_rows, in_rps, proc_rps, dur_ms, wm_delay),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[MetricsListener Warn] {e}")


def write_silver_and_timescale(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    dedup_df = batch_df.dropDuplicates(["record_id"]).withColumn("event_date", F.to_date("event_time_utc"))

    (
        dedup_df.write
        .mode("append")
        .partitionBy("category_id", "event_date")
        .parquet("hdfs://master:9000/datalake/cleaned/eonet")
    )

    rows = [
        (
            r.record_id, r.event_id, r.title, r.category_id, r.source_id,
            r.status, r.event_time_utc, r.first_seen_utc, r.closed_time_utc,
            r.ingested_at_utc, r.longitude, r.latitude, r.magnitude_value,
            r.magnitude_unit, r.temp_2m_c, r.humidity_2m_pct,
            r.wind_10m_ms, r.precip_mm_day
        )
        for r in dedup_df.collect()
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
            execute_values(cur, sql, rows, page_size=500)
            cur.execute("""
                UPDATE eonet_predictions p
                SET status = latest.status
                FROM (
                    SELECT DISTINCT ON (event_id) event_id, status
                    FROM eonet_events_stream
                    ORDER BY event_id, ingested_at_utc DESC
                ) latest
                WHERE p.event_id = latest.event_id
                  AND p.status IS DISTINCT FROM latest.status;
            """)
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("NASA-EONET-Structured-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(TimescaleMetricsListener())

    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_kafka
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("record_id").isNotNull() & F.col("event_time_utc").isNotNull())
        .withColumn("ingest_date", F.to_date("ingested_at_utc"))
    )

    bronze_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", "hdfs://master:9000/datalake/raw/eonet")
        .option("checkpointLocation", "hdfs://master:9000/checkpoints/eonet/bronze")
        .partitionBy("ingest_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    silver_stream = parsed.withWatermark("ingested_at_utc", "2 hours")

    silver_query = (
        silver_stream.writeStream
        .foreachBatch(write_silver_and_timescale)
        .option("checkpointLocation", "hdfs://master:9000/checkpoints/eonet/silver")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
