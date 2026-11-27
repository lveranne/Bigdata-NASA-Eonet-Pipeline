import os
import time
import json
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, percentile_approx
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from xgboost.spark import SparkXGBRegressor

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

spark = SparkSession.builder.appName("eonet_ml_train_benchmark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://master:9000/datalake/curated/eonet_features")
train_df = df.filter(col("data_split") == "train").cache()
test_df = df.filter(col("data_split") == "test").cache()

if test_df.count() < 20:
    train_df, eval_df = train_df.randomSplit([0.8, 0.2], seed=42)
else:
    eval_df = test_df

print(f"[ML] Train rows={train_df.count()}, Eval rows={eval_df.count()}")

cat_idx = StringIndexer(inputCol="category_id", outputCol="cat_idx", handleInvalid="keep")
src_idx = StringIndexer(inputCol="source_id", outputCol="src_idx", handleInvalid="keep")
ohe = OneHotEncoder(inputCols=["cat_idx", "src_idx"], outputCols=["cat_vec", "src_vec"])

NUMERIC_COLS = [
    "latitude", "longitude", "abs_latitude", "elapsed_hours",
    "doy_sin", "doy_cos", "magnitude_value",
    "temp_2m_c", "humidity_2m_pct", "wind_10m_ms", "precip_mm_day"
]
assembler = VectorAssembler(
    inputCols=["cat_vec", "src_vec"] + NUMERIC_COLS,
    outputCol="features",
    handleInvalid="keep"
)

rmse_eval = RegressionEvaluator(labelCol="y_dur", predictionCol="prediction", metricName="rmse")
mae_eval = RegressionEvaluator(labelCol="y_dur", predictionCol="prediction", metricName="mae")
f1_eval = MulticlassClassificationEvaluator(labelCol="y_sev", predictionCol="prediction", metricName="f1")

# 1. v1 BASELINE: Category Median Duration
t0 = time.time()
medians = (
    train_df.groupBy("category_id")
    .agg(percentile_approx("y_dur", 0.5).alias("prediction"))
)
medians.write.mode("overwrite").parquet("hdfs://master:9000/models/eonet/v1_baseline_medians")
v1_preds = eval_df.join(medians, on="category_id", how="left").fillna({"prediction": 72.0})
v1_rmse = rmse_eval.evaluate(v1_preds)
v1_mae = mae_eval.evaluate(v1_preds)
v1_time = round(time.time() - t0, 2)

# 2. v2 SPARK MLLIB CHAMPION: GBTRegressor (CrossValidator) + RandomForestClassifier
t0 = time.time()
gbt_reg = GBTRegressor(labelCol="y_dur", featuresCol="features", seed=42)
pipe_v2_dur = Pipeline(stages=[cat_idx, src_idx, ohe, assembler, gbt_reg])

grid = (
    ParamGridBuilder()
    .addGrid(gbt_reg.maxDepth, [3, 5])
    .addGrid(gbt_reg.maxIter, [20, 40])
    .build()
)
cv = CrossValidator(
    estimator=pipe_v2_dur,
    estimatorParamMaps=grid,
    evaluator=rmse_eval,
    numFolds=3,
    parallelism=2,
    seed=42,
)
cv_model = cv.fit(train_df)
best_v2_dur = cv_model.bestModel
best_v2_dur.write().overwrite().save("hdfs://master:9000/models/eonet/v2_duration")

v2_preds = best_v2_dur.transform(eval_df)
v2_rmse = rmse_eval.evaluate(v2_preds)
v2_mae = mae_eval.evaluate(v2_preds)
v2_time = round(time.time() - t0, 2)

rf_cls = RandomForestClassifier(labelCol="y_sev", featuresCol="features", numTrees=50, maxDepth=6, seed=42)
pipe_v2_sev = Pipeline(stages=[cat_idx, src_idx, ohe, assembler, rf_cls])
model_v2_sev = pipe_v2_sev.fit(train_df)
model_v2_sev.write().overwrite().save("hdfs://master:9000/models/eonet/v2_severity")
v2_f1 = f1_eval.evaluate(model_v2_sev.transform(eval_df))

# 3. v3 LECTURE 6 BENCHMARK: SparkXGBRegressor
t0 = time.time()
xgb_reg = SparkXGBRegressor(
    features_col="features",
    label_col="y_dur",
    num_workers=1,
    max_depth=5,
    n_estimators=40,
    learning_rate=0.1,
)
pipe_v3_dur = Pipeline(stages=[cat_idx, src_idx, ohe, assembler, xgb_reg])
model_v3_dur = pipe_v3_dur.fit(train_df)
model_v3_dur.write().overwrite().save("hdfs://master:9000/models/eonet/v3_xgboost_duration")

v3_preds = model_v3_dur.transform(eval_df)
v3_rmse = rmse_eval.evaluate(v3_preds)
v3_mae = mae_eval.evaluate(v3_preds)
v3_time = round(time.time() - t0, 2)

benchmark_summary = {
    "v1_baseline": {"train_sec": v1_time, "rmse_hours": round(v1_rmse, 2), "mae_hours": round(v1_mae, 2)},
    "v2_spark_mllib_gbt": {"train_sec": v2_time, "rmse_hours": round(v2_rmse, 2), "mae_hours": round(v2_mae, 2), "severity_f1": round(v2_f1, 4)},
    "v3_spark_xgboost": {"train_sec": v3_time, "rmse_hours": round(v3_rmse, 2), "mae_hours": round(v3_mae, 2)},
}
print("\n=== BENCHMARK RESULTS ===")
print(json.dumps(benchmark_summary, indent=2))

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "worker"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "eonet_db"),
    user=os.getenv("DB_USER", "eonet_user"),
    password=os.getenv("DB_PASSWORD", "eonet_pass"),
)
with conn, conn.cursor() as cur:
    cur.execute(
        """
        INSERT INTO eonet_lifecycle_events (event_ts, component, event_type, model_version, details)
        VALUES (NOW(), 'ml_pipeline', 'model_retrain_benchmark', 'v2_v3', %s::jsonb)
        """,
        (json.dumps(benchmark_summary),),
    )
conn.close()
spark.stop()
