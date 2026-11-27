import argparse
import os
import time
import json
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

parser = argparse.ArgumentParser(description="Benchmark Spark ML training time across 1, 2, and 4 YARN executors")
parser.add_argument("--executors", type=int, required=True, choices=[1, 2, 4])
parser.add_argument("--cores-per-executor", type=int, default=1)
parser.add_argument("--executor-memory", default="1g")
args = parser.parse_args()

t_total_start = time.time()
spark = (
    SparkSession.builder
    .appName(f"eonet_scaling_test_{args.executors}_executors")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Repartition to 8 partitions so parallelism across 1, 2, and 4 executors is fair
df = spark.read.parquet("hdfs://master:9000/datalake/curated/eonet_features") \
    .filter((col("status") == "closed") & col("elapsed_hours").isNotNull()) \
    .repartition(8)

train_df, eval_df = df.randomSplit([0.8, 0.2], seed=42)
train_count = train_df.count()
eval_count = eval_df.count()

cat_idx = StringIndexer(inputCol="category_id", outputCol="cat_idx", handleInvalid="keep")
src_idx = StringIndexer(inputCol="source_id", outputCol="src_idx", handleInvalid="keep")
ohe = OneHotEncoder(inputCols=["cat_idx", "src_idx"], outputCols=["cat_vec", "src_vec"])

num_cols = [c for c in ["latitude", "longitude", "abs_latitude", "magnitude_value", "temp_2m_c", "humidity_2m_pct", "wind_10m_ms", "precip_mm_day"] if c in df.columns]
assembler = VectorAssembler(inputCols=["cat_vec", "src_vec"] + num_cols, outputCol="features", handleInvalid="keep")

gbt = GBTRegressor(featuresCol="features", labelCol="elapsed_hours", maxIter=25, maxDepth=5, seed=42)
pipeline = Pipeline(stages=[cat_idx, src_idx, ohe, assembler, gbt])

t_fit_start = time.time()
model = pipeline.fit(train_df)
fit_duration_sec = round(time.time() - t_fit_start, 2)

preds = model.transform(eval_df)
rmse = round(RegressionEvaluator(labelCol="elapsed_hours", predictionCol="prediction", metricName="rmse").evaluate(preds), 2)
total_duration_sec = round(time.time() - t_total_start, 2)

print(f"[SCALING] executors={args.executors} | train_rows={train_count} | fit_sec={fit_duration_sec}s | total_sec={total_duration_sec}s | rmse={rmse}")

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
        CREATE TABLE IF NOT EXISTS eonet_scaling_benchmark (
            num_executors       INT PRIMARY KEY,
            cores_per_executor  INT NOT NULL,
            executor_memory     TEXT NOT NULL,
            train_rows          INT NOT NULL,
            eval_rows           INT NOT NULL,
            fit_duration_sec    DOUBLE PRECISION NOT NULL,
            total_duration_sec  DOUBLE PRECISION NOT NULL,
            speedup_vs_1_exec   DOUBLE PRECISION,
            parallel_efficiency DOUBLE PRECISION,
            rmse_hours          DOUBLE PRECISION NOT NULL,
            recorded_at_utc     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        INSERT INTO eonet_scaling_benchmark (
            num_executors, cores_per_executor, executor_memory, train_rows, eval_rows,
            fit_duration_sec, total_duration_sec, rmse_hours, recorded_at_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (num_executors) DO UPDATE SET
            cores_per_executor = EXCLUDED.cores_per_executor,
            executor_memory = EXCLUDED.executor_memory,
            train_rows = EXCLUDED.train_rows,
            eval_rows = EXCLUDED.eval_rows,
            fit_duration_sec = EXCLUDED.fit_duration_sec,
            total_duration_sec = EXCLUDED.total_duration_sec,
            rmse_hours = EXCLUDED.rmse_hours,
            recorded_at_utc = NOW();
        """,
        (args.executors, args.cores_per_executor, args.executor_memory, train_count, eval_count, fit_duration_sec, total_duration_sec, rmse),
    )
    # Recompute Speedup = T(1) / T(N) and Parallel Efficiency = Speedup / N
    cur.execute(
        """
        WITH base AS (SELECT fit_duration_sec AS t1 FROM eonet_scaling_benchmark WHERE num_executors = 1)
        UPDATE eonet_scaling_benchmark b
        SET speedup_vs_1_exec = ROUND((base.t1 / b.fit_duration_sec)::numeric, 2),
            parallel_efficiency = ROUND(((base.t1 / b.fit_duration_sec) / b.num_executors * 100.0)::numeric, 1)
        FROM base;
        """
    )

conn.close()
spark.stop()
