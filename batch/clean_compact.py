from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_timestamp, when, lit, coalesce, row_number

spark = SparkSession.builder.appName("eonet_clean_compact").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_df = spark.read.parquet("hdfs://master:9000/datalake/raw/eonet")

w = Window.partitionBy("record_id").orderBy(col("temp_2m_c").desc_nulls_last(), col("ingested_at_utc").desc())

dedup_df = (
    raw_df
    .filter(col("record_id").isNotNull() & col("event_id").isNotNull())
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

cleaned_df = (
    dedup_df
    .withColumn("event_ts", to_timestamp("event_time_utc"))
    .withColumn("first_seen_ts", to_timestamp("first_seen_utc"))
    .withColumn("closed_ts", to_timestamp("closed_time_utc"))
    .withColumn("ingested_ts", to_timestamp("ingested_at_utc"))
    .withColumn("magnitude_value", coalesce(col("magnitude_value"), lit(0.0)))
    .withColumn("temp_2m_c", coalesce(col("temp_2m_c"), lit(15.0)))
    .withColumn("humidity_2m_pct", coalesce(col("humidity_2m_pct"), lit(50.0)))
    .withColumn("wind_10m_ms", coalesce(col("wind_10m_ms"), lit(3.0)))
    .withColumn("precip_mm_day", coalesce(col("precip_mm_day"), lit(0.0)))
    .withColumn(
        "data_split",
        when(col("closed_ts") < to_timestamp(lit("2026-11-27T00:00:00Z")), lit("train"))
        .otherwise(lit("test"))
    )
)

cleaned_df.coalesce(2).write.mode("overwrite").parquet("hdfs://master:9000/datalake/cleaned/eonet")
print(f"[CLEANED] Wrote {cleaned_df.count()} deduplicated records to /datalake/cleaned/eonet.")
spark.stop()
