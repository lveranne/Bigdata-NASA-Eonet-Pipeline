#!/usr/bin/env bash
source /etc/profile.d/bigdata.sh
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1g \
  --conf spark.executor.memoryOverhead=384m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /home/ubuntu/Bigdata-NASA-Eonet-Pipeline/streaming/ingest_raw.py
