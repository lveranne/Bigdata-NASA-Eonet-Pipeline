import os
import sys
import json
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit, unix_timestamp, greatest
)
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.ml import PipelineModel

sys.path.append(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/batch"))
from build_features import engineer_features

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

spark = SparkSession.builder.appName("eonet_speed_scoring").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

model_v2_dur = PipelineModel.load("hdfs://master:9000/models/eonet/v2_duration")
model_v2_sev = PipelineModel.load("hdfs://master:9000/models/eonet/v2_severity")
model_v3_dur = PipelineModel.load("hdfs://master:9000/models/eonet/v3_xgboost_duration")
v1_medians_df = spark.read.parquet("hdfs://master:9000/models/eonet/v1_baseline_medians")

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

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "master:9092")
    .option("subscribe", "nasa-eonet-raw")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw_stream.select(from_json(col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp("event_time_utc"))
    .withColumn("first_seen_ts", to_timestamp("first_seen_utc"))
    .withColumn("closed_ts", to_timestamp("closed_time_utc"))
    .withColumn("ingested_ts", to_timestamp("ingested_at_utc"))
    .withWatermark("event_ts", "2 hours")
    .dropDuplicates(["record_id"])
    .fillna({
        "magnitude_value": 0.0,
        "temp_2m_c": 15.0,
        "humidity_2m_pct": 50.0,
        "wind_10m_ms": 3.0,
        "precip_mm_day": 0.0,
    })
)


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "worker"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "eonet_db"),
        user=os.getenv("DB_USER", "eonet_user"),
        password=os.getenv("DB_PASSWORD", "eonet_pass"),
    )


def process_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Score live open events and post-cutoff closed test events (>= 2026-11-27)
    live_or_test = batch_df.filter(
        (col("status") == "open") | (col("closed_ts") >= to_timestamp(lit("2026-11-27T00:00:00Z")))
    )
    if live_or_test.rdd.isEmpty():
        return

    featured = engineer_features(live_or_test).withColumn(
        "data_split",
        when(col("closed_ts") < to_timestamp(lit("2026-11-27T00:00:00Z")), lit("train")).otherwise(lit("test"))
    ).withColumn(
        "actual_duration_hours",
        when(
            (col("status") == "closed") & col("closed_ts").isNotNull(),
            greatest(lit(1.0), (unix_timestamp(col("closed_ts")) - unix_timestamp(col("first_seen_ts"))) / 3600.0)
        ).otherwise(lit(None))
    )

    ml_temp_cols = ["cat_idx", "src_idx", "cat_vec", "src_vec", "features"]
    scored_v2 = model_v2_dur.transform(featured).withColumnRenamed("prediction", "pred_v2").drop(*ml_temp_cols)
    scored_v2_sev = model_v2_sev.transform(scored_v2).withColumnRenamed("prediction", "pred_sev").drop(*ml_temp_cols, "rawPrediction", "probability")
    scored_v3 = model_v3_dur.transform(scored_v2_sev).withColumnRenamed("prediction", "pred_v3").drop(*ml_temp_cols)
    scored_all = scored_v3.join(v1_medians_df.withColumnRenamed("prediction", "pred_v1"), on="category_id", how="left").fillna({"pred_v1": 72.0})

    rows = scored_all.select(
        "record_id", "event_id", "title", "category_id", "source_id", "status",
        "event_ts", "first_seen_ts", "closed_ts", "ingested_ts",
        "latitude", "longitude", "magnitude_value", "magnitude_unit",
        "temp_2m_c", "humidity_2m_pct", "wind_10m_ms", "precip_mm_day",
        "elapsed_hours", "actual_duration_hours", "data_split",
        "pred_v1", "pred_v2", "pred_v3", "pred_sev"
    ).collect()

    conn = get_db_conn()
    with conn, conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO eonet_events_stream (
                    record_id, event_id, title, category_id, source_id, status,
                    event_time_utc, first_seen_utc, closed_time_utc, ingested_at_utc,
                    latitude, longitude, magnitude_value, magnitude_unit,
                    temp_2m_c, humidity_2m_pct, wind_10m_ms, precip_mm_day, data_split
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (record_id, event_time_utc) DO UPDATE SET status = EXCLUDED.status, closed_time_utc = EXCLUDED.closed_time_utc
                """,
                (
                    r.record_id, r.event_id, r.title, r.category_id, r.source_id, r.status,
                    r.event_ts, r.first_seen_ts, r.closed_ts, r.ingested_ts,
                    r.latitude, r.longitude, r.magnitude_value, r.magnitude_unit,
                    r.temp_2m_c, r.humidity_2m_pct, r.wind_10m_ms, r.precip_mm_day, r.data_split
                ),
            )

            for ver, pred_val in [("v1_baseline", r.pred_v1), ("v2_gbt", r.pred_v2), ("v3_xgboost", r.pred_v3)]:
                pred_dur = float(max(1.0, pred_val))
                rem_hours = max(0.0, pred_dur - float(r.elapsed_hours or 0.0))
                act_dur = float(r.actual_duration_hours) if r.actual_duration_hours is not None else None
                abs_err = abs(pred_dur - act_dur) if act_dur is not None else None
                sq_err = (abs_err ** 2) if abs_err is not None else None

                cur.execute(
                    """
                    INSERT INTO eonet_predictions (
                        event_id, model_version, predicted_at_utc, category_id, status,
                        elapsed_hours, pred_duration_hours, pred_remaining_hours, pred_severity_tier,
                        actual_duration_hours, abs_error_hours, closed_time_utc, data_split
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id, model_version, predicted_at_utc) DO UPDATE SET
                        status = EXCLUDED.status,
                        actual_duration_hours = EXCLUDED.actual_duration_hours,
                        abs_error_hours = EXCLUDED.abs_error_hours,
                        closed_time_utc = EXCLUDED.closed_time_utc
                    """,
                    (
                        r.event_id, ver, r.event_ts, r.category_id, r.status,
                        r.elapsed_hours, pred_dur, rem_hours, int(r.pred_sev),
                        act_dur, abs_err, r.closed_ts, r.data_split
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO eonet_prediction_history (
                        record_id, event_id, model_version, scored_at_utc, category_id, status,
                        pred_duration_hours, actual_duration_hours, abs_error_hours, squared_error_hours,
                        pred_severity_tier, data_split
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (record_id, model_version, scored_at_utc) DO NOTHING
                    """,
                    (
                        r.record_id, r.event_id, ver, r.event_ts, r.category_id, r.status,
                        pred_dur, act_dur, abs_err, sq_err,
                        int(r.pred_sev), r.data_split
                    ),
                )
    conn.close()


c = get_db_conn()
with c, c.cursor() as cur:
    cur.execute(
        "INSERT INTO eonet_lifecycle_events (event_ts, component, event_type, model_version, details) VALUES (NOW(), 'eonet_stream', 'stream_start', 'v3_xgboost', '{\"checkpoint\": \"/checkpoints/scoring\"}'::jsonb)"
    )
c.close()

(
    events.writeStream
    .foreachBatch(process_micro_batch)
    .option("checkpointLocation", "hdfs://master:9000/checkpoints/scoring")
    .trigger(processingTime="1 minute")
    .start()
    .awaitTermination()
)
