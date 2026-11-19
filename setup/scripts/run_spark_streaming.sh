#!/usr/bin/env bash
set -e
cd /home/ubuntu/Bigdata-NASA-Eonet-Pipeline
SPARK_VER=$(ls /opt/spark/jars/spark-core_2.13-*.jar | sed -E 's/.*spark-core_2\.13-([0-9]+\.[0-9]+\.[0-9]+)\.jar/\1/')

exec /opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --num-executors 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VER} \
  /home/ubuntu/Bigdata-NASA-Eonet-Pipeline/spark_streaming/eonet_stream.py
