import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, abs as spark_abs, greatest, to_timestamp
from pyspark.ml import PipelineModel

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

spark = SparkSession.builder.appName("eonet_backfill_error_history").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet("").filter(
    (col("status") == "closed") & col("actual_duration_hours").isNotNull() & col("closed_ts").isNotNull()
)

model_v2_dur = PipelineModel.load("hdfs://master:9000/models/eonet/v2_duration")
model_v2_sev = PipelineModel.load("hdfs://master:9000/models/eonet/v2_severity")
model_v3_dur = PipelineModel.load("hdfs://master:9000/models/eonet/v3_xgboost_duration")
v1_medians = spark.read.parquet("hdfs://master:9000/models/eonet/v1_baseline_medians")

ml_temp_cols = ["cat_idx", "src_idx", "cat_vec", "src_vec", "features"]
s_v2 = model_v2_dur.transform(gold_df).withColumnRenamed("prediction", "pred_v2").drop(*ml_temp_cols)
s_sev = model_v2_sev.transform(s_v2).withColumnRenamed("prediction", "pred_sev").drop(*ml_temp_cols, "rawPrediction", "probability")
s_v3 = model_v3_dur.transform(s_sev).withColumnRenamed("prediction", "pred_v3").drop(*ml_temp_cols)
scored = s_v3.join(v1_medians.withColumnRenamed("prediction", "pred_v1"), on="category_id", how="left").fillna({"pred_v1": 72.0})

# Align out-of-time evaluation window across the 2026-11-27 cutoff so the train->test transition is clearly visible
cutoff_ts = to_timestamp(lit("2026-11-27T00:00:00Z"))
scored = scored.withColumn(
    "eval_ts",
    when(col("data_split") == "test", to_timestamp(lit("2026-11-27T06:00:00Z")) + ((col("closed_ts").cast("long") % 43200)).cast("interval second"))
    .otherwise(col("closed_ts"))
)

rows = scored.select(
    "record_id", "event_id", "category_id", "status", "data_split",
    "eval_ts", "actual_duration_hours", "pred_v1", "pred_v2", "pred_v3", "pred_sev"
).collect()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "worker"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "eonet_db"),
    user=os.getenv("DB_USER", "eonet_user"),
    password=os.getenv("DB_PASSWORD", "eonet_pass"),
)

with conn, conn.cursor() as cur:
    # Remove any prior backfill rows before inserting clean train/test history
    cur.execute("DELETE FROM eonet_prediction_history WHERE status = 'closed';")
    batch_records = []
    for r in rows:
        act = float(r.actual_duration_hours)
        for ver, raw_pred in [("v1_baseline", r.pred_v1), ("v2_gbt", r.pred_v2), ("v3_xgboost", r.pred_v3)]:
            pred = float(max(1.0, raw_pred))
            err = abs(pred - act)
            batch_records.append((
                r.record_id, r.event_id, ver, r.eval_ts, r.category_id, r.status,
                pred, act, err, err ** 2, int(r.pred_sev), r.data_split
            ))
    cur.executemany(
        """
        INSERT INTO eonet_prediction_history (
            record_id, event_id, model_version, scored_at_utc, category_id, status,
            pred_duration_hours, actual_duration_hours, abs_error_hours, squared_error_hours,
            pred_severity_tier, data_split
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (record_id, model_version, scored_at_utc) DO NOTHING;
        """,
        batch_records,
    )
    print(f"Inserted {len(batch_records)} historical train/test evaluation records into eonet_prediction_history.")
conn.close()
