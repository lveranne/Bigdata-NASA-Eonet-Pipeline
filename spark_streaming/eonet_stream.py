import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
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
    StructField("record_id", StringType(), False),
    StructField("event_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("category_id", StringType(), False),
    StructField("source_id", StringType(), True),
    StructField("status", StringType(), False),
    StructField("event_time_utc", StringType(), False),
    StructField("first_seen_utc", StringType(), True),
    StructField("closed_time_utc", StringType(), True),
    StructField("ingested_at_utc", StringType(), False),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("magnitude_value", DoubleType(), True),
    StructField("magnitude_unit", StringType(), True),
    StructField("temp_2m_c", DoubleType(), True),
    StructField("humidity_2m_pct", DoubleType(), True),
    StructField("wind_10m_ms", DoubleType(), True),
    StructField("precip_mm_day", DoubleType(), True),
])


def upsert_to_timescaledb(batch_df, batch_id: int) -> None:
    """Idempotently upsert each Spark micro-batch into TimescaleDB."""
    if batch_df.rdd.isEmpty():
        return

    rows = [
        (
            r.record_id, r.event_id, r.title, r.category_id, r.source_id,
            r.status, r.event_time_utc, r.first_seen_utc, r.closed_time_utc,
            r.ingested_at_utc, r.longitude, r.latitude, r.magnitude_value,
            r.magnitude_unit, r.temp_2m_c, r.humidity_2m_pct,
            r.wind_10m_ms, r.precip_mm_day
        )
        for r in batch_df.collect()
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
        host=TS_HOST,
        port=TS_PORT,
        dbname=TS_DB,
        user=TS_USER,
        password=TS_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
        print(f"[Batch {batch_id}] Upserted {len(rows)} records into TimescaleDB.")
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

    # 1. Read raw stream from Kafka
    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON and cast UTC timestamps
    parsed = (
        raw_kafka
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("record_id").isNotNull() & F.col("event_time_utc").isNotNull())
        .withColumn("event_time_utc", F.to_timestamp("event_time_utc"))
        .withColumn("first_seen_utc", F.to_timestamp("first_seen_utc"))
        .withColumn("closed_time_utc", F.to_timestamp("closed_time_utc"))
        .withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc"))
        .withColumn("event_date", F.to_date("event_time_utc"))
    )

    # 3. Sink A: Raw Bronze Parquet in HDFS (partitioned by event_date)
    raw_hdfs_query = (
        parsed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "hdfs://master:9000/datalake/raw/eonet")
        .option("checkpointLocation", "hdfs://master:9000/checkpoints/eonet_raw")
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 4. Deduplicate within a 2-hour watermark for Silver & TimescaleDB
    cleaned = (
        parsed
        .withWatermark("event_time_utc", "2 hours")
        .dropDuplicates(["record_id"])
    )

    # 5. Sink B: Cleaned Silver Parquet in HDFS (partitioned by category_id, event_date)
    cleaned_hdfs_query = (
        cleaned.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "hdfs://master:9000/datalake/cleaned/eonet")
        .option("checkpointLocation", "hdfs://master:9000/checkpoints/eonet_cleaned")
        .partitionBy("category_id", "event_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 6. Sink C: Real-time TimescaleDB upsert on worker
    timescale_query = (
        cleaned.writeStream
        .foreachBatch(upsert_to_timescaledb)
        .outputMode("update")
        .option("checkpointLocation", "hdfs://master:9000/checkpoints/eonet_timescale")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
