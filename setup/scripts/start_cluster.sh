#!/usr/bin/env bash
set -e

echo "[1/4] Starting Kafka (KRaft) & EONET Producer on master..."
sudo systemctl start kafka
sleep 12
sudo systemctl start eonet-producer

echo "[2/4] Starting HDFS (NameNode + 2 DataNodes)..."
/opt/hadoop/sbin/start-dfs.sh

echo "[3/4] Starting YARN (ResourceManager + 2 NodeManagers)..."
/opt/hadoop/sbin/start-yarn.sh

echo "[4/4] Running cluster health check..."
bash "$(dirname "$0")/health_check.sh"
