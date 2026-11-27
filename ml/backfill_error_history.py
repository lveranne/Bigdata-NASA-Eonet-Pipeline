import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp
from pyspark.ml import PipelineModel

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

spark = SparkSession.builder.appName("eonet_backfill_error_history").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet("hdfs://master:9000/datalake/curated/eonet_features").filter(
    (col("status") == "closed") & col("elapsed_hours").isNotNull() & col("closed_ts").isNotNull()
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

# Chronological 80/20 temporal split across historical closed hazards + live post-cutoff test hazards
split_epoch = scored.approxQuantile("closed_ts_epoch", [0.80], 0.01)[0] if "closed_ts_epoch" in scored.columns else None
if split_epoch is None:
    scored = scored.withColumn("closed_epoch", col("closed_ts").cast("long"))
    split_epoch = scored.approxQuantile("closed_epoch", [0.80], 0.01)[0]
else:
    scored = scored.withColumn("closed_epoch", col("closed_ts").cast("long"))

scored = scored.withColumn(
    "eval_split",
    when((col("data_split") == "test") | (col("closed_epoch") >= lit(split_epoch)), lit("test")).otherwise(lit("train"))
).withColumn(
    "eval_ts",
    when(col("data_split") == "test", to_timestamp(lit("2026-11-27T06:00:00Z")) + ((col("closed_epoch") % 43200)).cast("interval second"))
    .otherwise(col("closed_ts"))
)

rows = scored.select(
    "record_id", "event_id", "category_id", "status", "eval_split",
    "eval_ts", "elapsed_hours", "pred_v1", "pred_v2", "pred_v3", "pred_sev"
).collect()

# Compute exact split cutoff timestamp for Grafana annotation
cutoff_row = scored.filter(col("eval_split") == "test").agg({"eval_ts": "min"}).collect()[0][0]

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "worker"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "eonet_db"),
    user=os.getenv("DB_USER", "eonet_user"),
    password=os.getenv("DB_PASSWORD", "eonet_pass"),
)

with conn, conn.cursor() as cur:
    cur.execute("DELETE FROM eonet_prediction_history WHERE status = 'closed';")
    if cutoff_row is not None:
        cur.execute(
            """
            INSERT INTO eonet_lifecycle_events (event_ts, component, event_type, model_version, details)
            VALUES (%s, 'ml_evaluation', 'temporal_train_test_split', 'all', '{"split_ratio": "80% train / 20% temporal test"}'::jsonb)
            ON CONFLICT DO NOTHING;
            """,
            (cutoff_row,),
        )
    batch_records = []
    for r in rows:
        act = float(r.elapsed_hours)
        for ver, raw_pred in [("v1_baseline", r.pred_v1), ("v2_gbt", r.pred_v2), ("v3_xgboost", r.pred_v3)]:
            pred = float(max(1.0, raw_pred))
            err = abs(pred - act)
            batch_records.append((
                r.record_id, r.event_id, ver, r.eval_ts, r.category_id, r.status,
                pred, act, err, err ** 2, int(r.pred_sev), r.eval_split
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
    print(f"Inserted {len(batch_records)} historical train/test evaluation records into eonet_prediction_history (split cutoff={cutoff_row}).")
conn.close()
