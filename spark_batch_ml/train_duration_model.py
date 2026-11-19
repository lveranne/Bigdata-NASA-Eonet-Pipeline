import argparse
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--executors-label", type=str, default="2", help="Executor count label for scaling log")
    parser.add_argument("--skip-save", action="store_true", help="Skip overwriting HDFS model during scaling runs")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"NASA-EONET-ML-Train-Exec-{args.executors_label}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Silver Parquet from HDFS and collapse to one row per unique closed event_id
    df = spark.read.parquet("hdfs://master:9000/datalake/silver/eonet")
    closed_events = (
        df.filter(
            (F.col("status") == "closed")
            & F.col("first_seen_utc").isNotNull()
            & F.col("closed_time_utc").isNotNull()
        )
        .groupBy("event_id", "category_id")
        .agg(
            F.min("first_seen_utc").alias("start_utc"),
            F.max("closed_time_utc").alias("end_utc"),
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
            F.max("magnitude_value").alias("magnitude_value"),
            F.avg("temp_2m_c").alias("temp_2m_c"),
            F.avg("humidity_2m_pct").alias("humidity_2m_pct"),
            F.avg("wind_10m_ms").alias("wind_10m_ms"),
            F.avg("precip_mm_day").alias("precip_mm_day"),
        )
        .withColumn("duration_hours", (F.unix_timestamp("end_utc") - F.unix_timestamp("start_utc")) / 3600.0)
        .filter((F.col("duration_hours") > 0.0) & (F.col("duration_hours") <= 24 * 365))
        .withColumn("start_month", F.month("start_utc").cast("double"))
        .withColumn("abs_latitude", F.abs(F.col("latitude")))
        .withColumn("has_magnitude", F.when(F.col("magnitude_value").isNotNull(), 1.0).otherwise(0.0))
        .repartition(8)
        .cache()
    )

    total_rows = closed_events.count()
    train_df, test_df = closed_events.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    test_df.cache()

    rmse_eval = RegressionEvaluator(labelCol="duration_hours", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="duration_hours", predictionCol="prediction", metricName="mae")

    # 2. BASELINE: Category Mean Duration (fallback to global training mean)
    global_mean = train_df.agg(F.avg("duration_hours")).first()[0]
    cat_means = train_df.groupBy("category_id").agg(F.avg("duration_hours").alias("cat_mean"))
    baseline_preds = (
        test_df.join(cat_means, on="category_id", how="left")
        .withColumn("prediction", F.coalesce(F.col("cat_mean"), F.lit(global_mean)))
    )
    base_rmse = float(rmse_eval.evaluate(baseline_preds))
    base_mae = float(mae_eval.evaluate(baseline_preds))
    print(f"[Baseline: Category Mean] Events={total_rows} | Test RMSE={base_rmse:.2f} hr | Test MAE={base_mae:.2f} hr")

    # 3. SPARK ML PIPELINE + PARALLEL CROSSVALIDATOR (parallelism=2)
    numeric_cols = [
        "latitude", "abs_latitude", "longitude", "start_month",
        "has_magnitude", "magnitude_value",
        "temp_2m_c", "humidity_2m_pct", "wind_10m_ms", "precip_mm_day"
    ]
    imputed_cols = [f"{c}_imp" for c in numeric_cols]

    imputer = Imputer(strategy="median", inputCols=numeric_cols, outputCols=imputed_cols)
    cat_indexer = StringIndexer(inputCol="category_id", outputCol="category_idx", handleInvalid="keep")
    cat_encoder = OneHotEncoder(inputCols=["category_idx"], outputCols=["category_vec"])
    assembler = VectorAssembler(inputCols=["category_vec"] + imputed_cols, outputCol="features", handleInvalid="keep")
    gbt = GBTRegressor(featuresCol="features", labelCol="duration_hours", seed=42)

    pipeline = Pipeline(stages=[imputer, cat_indexer, cat_encoder, assembler, gbt])

    param_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [3, 5])
        .addGrid(gbt.maxIter, [20, 40])
        .addGrid(gbt.stepSize, [0.05, 0.1])
        .build()
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=rmse_eval,
        numFolds=3,
        parallelism=2,
        seed=42,
    )

    t0 = time.perf_counter()
    cv_model = cv.fit(train_df)
    train_seconds = round(time.perf_counter() - t0, 2)

    best_pipeline = cv_model.bestModel
    test_preds = best_pipeline.transform(test_df)
    gbt_rmse = float(rmse_eval.evaluate(test_preds))
    gbt_mae = float(mae_eval.evaluate(test_preds))

    print(f"[Tuned GBTRegressor | Executors={args.executors_label}] "
          f"Train Time={train_seconds}s | Test RMSE={gbt_rmse:.2f} hr | Test MAE={gbt_mae:.2f} hr")

    # 4. Append scaling & accuracy metrics to HDFS
    run_row = [(
        datetime.now(timezone.utc).isoformat(),
        str(args.executors_label),
        int(total_rows),
        train_seconds,
        base_rmse,
        base_mae,
        gbt_rmse,
        gbt_mae,
    )]
    log_df = spark.createDataFrame(
        run_row,
        ["run_utc", "executors", "dataset_rows", "train_seconds",
         "baseline_rmse", "baseline_mae", "gbt_rmse", "gbt_mae"],
    )
    log_df.write.mode("append").parquet("hdfs://master:9000/datalake/curated/ml_benchmarks")

    if not args.skip_save:
        best_pipeline.write().overwrite().save("hdfs://master:9000/models/eonet/duration_pipeline")
        print("Saved best tuned PipelineModel to hdfs://master:9000/models/eonet/duration_pipeline")
        import subprocess
        subprocess.run([
            "/home/ubuntu/Bigdata-NASA-Eonet-Pipeline/venv/bin/python",
            "/home/ubuntu/Bigdata-NASA-Eonet-Pipeline/setup/scripts/log_lifecycle_event.py",
            "--type", "MODEL_RETRAIN",
            "--component", "GBTRegressor",
            "--title", f"Duration Model Retrained (MAE={gbt_mae:.2f}h)",
            "--details", f"Rows={total_rows}, Executors={args.executors_label}, RMSE={gbt_rmse:.2f}h, MAE={gbt_mae:.2f}h",
            "--tags", "retrain,gbt,duration"
        ], check=False)


if __name__ == "__main__":
    main()
