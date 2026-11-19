import os
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

load_dotenv()

TS_HOST = os.getenv("TIMESCALE_HOST", "worker")
TS_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TS_DB = os.getenv("TIMESCALE_DB", "eonet_db")
TS_USER = os.getenv("TIMESCALE_USER", "eonet_user")
TS_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "eonet_pass")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("NASA-EONET-Severity-Classifier")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("hdfs://master:9000/datalake/silver/eonet")

    all_events = (
        df.groupBy("event_id", "category_id")
        .agg(
            F.last("status", ignorenulls=True).alias("status"),
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
        .withColumn("start_month", F.month("start_utc").cast("double"))
        .withColumn("abs_latitude", F.abs(F.col("latitude")))
        .withColumn("has_magnitude", F.when(F.col("magnitude_value").isNotNull(), 1.0).otherwise(0.0))
        .withColumn("duration_hours", (F.unix_timestamp("end_utc") - F.unix_timestamp("start_utc")) / 3600.0)
        .withColumn(
            "severity_class",
            F.when(F.col("duration_hours") < 48.0, 0.0)
            .when(F.col("duration_hours") <= 168.0, 1.0)
            .otherwise(2.0),
        )
    )

    labeled = (
        all_events.filter(
            (F.col("status") == "closed")
            & F.col("start_utc").isNotNull()
            & F.col("end_utc").isNotNull()
            & (F.col("duration_hours") > 0.0)
            & (F.col("duration_hours") <= 24 * 365)
        )
        .cache()
    )

    train_df, test_df = labeled.randomSplit([0.8, 0.2], seed=42)

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
    rf = RandomForestClassifier(featuresCol="features", labelCol="severity_class", seed=42)

    pipeline = Pipeline(stages=[imputer, cat_indexer, cat_encoder, assembler, rf])

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [30, 60])
        .addGrid(rf.maxDepth, [5, 8])
        .build()
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="severity_class", predictionCol="prediction", metricName="f1"
    )
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=f1_eval,
        numFolds=3,
        parallelism=2,
        seed=42,
    )

    cv_model = cv.fit(train_df)
    best_clf = cv_model.bestModel
    test_f1 = float(f1_eval.evaluate(best_clf.transform(test_df)))
    print(f"[RandomForestClassifier] Test Weighted F1-Score = {test_f1:.4f}")

    best_clf.write().overwrite().save("hdfs://master:9000/models/eonet/severity_classifier")
    import subprocess
    subprocess.run([
        "/home/ubuntu/Bigdata-NASA-Eonet-Pipeline/venv/bin/python",
        "/home/ubuntu/Bigdata-NASA-Eonet-Pipeline/setup/scripts/log_lifecycle_event.py",
        "--type", "MODEL_RETRAIN",
        "--component", "RandomForestClassifier",
        "--title", f"Severity Classifier Retrained (Weighted F1={test_f1:.4f})",
        "--details", f"3-class severity model updated on HDFS (Weighted F1={test_f1:.4f})",
        "--tags", "retrain,rf,severity"
    ], check=False)

    # Combine duration predictions (GBTRegressor) + severity tier predictions (RandomForestClassifier)
    dur_model = PipelineModel.load("hdfs://master:9000/models/eonet/duration_pipeline")
    dur_scored = (
        dur_model.transform(all_events)
        .withColumn("predicted_duration_hr", F.greatest(F.lit(1.0), F.round(F.col("prediction"), 2)))
        .drop("features", "prediction", "category_idx", "category_vec", *imputed_cols)
    )

    full_scored = (
        best_clf.transform(dur_scored)
        .withColumn("predicted_severity", F.col("prediction").cast("int"))
        .withColumn(
            "severity_label",
            F.when(F.col("predicted_severity") == 0, F.lit("Low"))
            .when(F.col("predicted_severity") == 1, F.lit("Moderate"))
            .otherwise(F.lit("High")),
        )
        .withColumn("scored_at_utc", F.lit(datetime.now(timezone.utc)))
    )

    db_rows = [
        (
            r.event_id, r.category_id, r.status,
            float(r.predicted_duration_hr), int(r.predicted_severity),
            r.severity_label, r.scored_at_utc
        )
        for r in full_scored.select(
            "event_id", "category_id", "status",
            "predicted_duration_hr", "predicted_severity", "severity_label", "scored_at_utc"
        ).collect()
    ]

    sql = """
        INSERT INTO eonet_predictions (
            event_id, category_id, status,
            predicted_duration_hr, predicted_severity, severity_label, scored_at_utc
        ) VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            category_id = EXCLUDED.category_id,
            status = EXCLUDED.status,
            predicted_duration_hr = EXCLUDED.predicted_duration_hr,
            predicted_severity = EXCLUDED.predicted_severity,
            severity_label = EXCLUDED.severity_label,
            scored_at_utc = EXCLUDED.scored_at_utc;
    """
    conn = psycopg2.connect(
        host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, db_rows, page_size=1000)
        conn.commit()
        print(f"Upserted {len(db_rows)} combined duration + severity predictions into TimescaleDB.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
