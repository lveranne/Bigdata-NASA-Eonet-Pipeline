from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("eonet_ingest_raw")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("schema_version", StringType())
    .add("record_id", StringType())
    .add("event_id", StringType())
    .add("title", StringType())
    .add("category_id", StringType())
    .add("source_id", StringType())
    .add("status", StringType())
    .add("event_time_utc", StringType())
    .add("first_seen_utc", StringType())
    .add("closed_time_utc", StringType())
    .add("ingested_at_utc", StringType())
    .add("longitude", DoubleType())
    .add("latitude", DoubleType())
    .add("magnitude_value", DoubleType())
    .add("magnitude_unit", StringType())
    .add("temp_2m_c", DoubleType())
    .add("humidity_2m_pct", DoubleType())
    .add("wind_10m_ms", DoubleType())
    .add("precip_mm_day", DoubleType())
)

raw_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "master:9092")
    .option("subscribe", "nasa-eonet-raw")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_kafka.select(
        from_json(col("value").cast("string"), schema).alias("e"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )
    .select("e.*", "kafka_partition", "kafka_offset")
    .withColumn("lake_landed_utc", current_timestamp())
    .withColumn("date", to_date(col("event_time_utc")))
)

query = (
    parsed.writeStream.format("parquet")
    .option("path", "hdfs://master:9000/datalake/raw/eonet")
    .option("checkpointLocation", "hdfs://master:9000/checkpoints/raw_eonet")
    .partitionBy("date")
    .trigger(processingTime="2 minutes")
    .start()
)

query.awaitTermination()
