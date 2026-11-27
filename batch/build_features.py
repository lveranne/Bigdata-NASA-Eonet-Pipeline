import math
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, abs as sql_abs, dayofyear, sin, cos, lit, when, unix_timestamp, greatest
)


def engineer_features(df: DataFrame) -> DataFrame:
    two_pi_over_365 = 2.0 * math.pi / 365.25
    doy = dayofyear(col("event_ts"))

    return (
        df
        .withColumn("abs_latitude", sql_abs(col("latitude")))
        .withColumn(
            "elapsed_hours",
            greatest(
                lit(0.0),
                (unix_timestamp(col("event_ts")) - unix_timestamp(col("first_seen_ts"))) / 3600.0
            )
        )
        .withColumn("doy_sin", sin(doy * lit(two_pi_over_365)))
        .withColumn("doy_cos", cos(doy * lit(two_pi_over_365)))
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("eonet_build_features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    cleaned = spark.read.parquet("hdfs://master:9000/datalake/cleaned/eonet")
    featured = engineer_features(cleaned)

    closed_events = (
        featured
        .filter((col("status") == "closed") & col("closed_ts").isNotNull())
        .withColumn(
            "y_dur",
            greatest(lit(1.0), (unix_timestamp(col("closed_ts")) - unix_timestamp(col("first_seen_ts"))) / 3600.0)
        )
        .withColumn(
            "y_sev",
            when((col("y_dur") >= 168.0) | (col("magnitude_value") >= 65.0) | (col("wind_10m_ms") >= 17.0), lit(2))
            .when((col("y_dur") >= 48.0) | (col("magnitude_value") >= 25.0) | (col("precip_mm_day") >= 20.0), lit(1))
            .otherwise(lit(0))
        )
    )

    closed_events.coalesce(2).write.mode("overwrite").parquet("hdfs://master:9000/datalake/curated/eonet_features")
    print(f"[CURATED] Saved {closed_events.count()} labeled events to /datalake/curated/eonet_features.")
    spark.stop()
