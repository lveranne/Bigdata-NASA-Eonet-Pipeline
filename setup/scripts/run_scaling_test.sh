#!/usr/bin/env bash
set -e
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_DRIVER_PYTHON=/home/ubuntu/Bigdata-NASA-Eonet-Pipeline/venv/bin/python
export PYSPARK_PYTHON=python3

for EXECS in 1 2 4; do
  echo "=========================================================="
  echo " Running Parallel CrossValidator Scaling Test: ${EXECS} Executor(s)"
  echo "=========================================================="
  SKIP_FLAG="--skip-save"
  if [ "$EXECS" -eq 4 ]; then
    SKIP_FLAG=""
  fi

  /opt/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors ${EXECS} \
    /home/ubuntu/Bigdata-NASA-Eonet-Pipeline/spark_batch_ml/train_duration_model.py \
    --executors-label ${EXECS} ${SKIP_FLAG}
done
