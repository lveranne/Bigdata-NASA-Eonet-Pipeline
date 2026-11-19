#!/usr/bin/env bash
echo "=== 1. Systemd Services (master) ==="
systemctl is-active kafka eonet-producer

echo "=== 2. Kafka Topic Metadata ==="
/opt/kafka/bin/kafka-topics.sh --describe --topic nasa-eonet-raw --bootstrap-server master:9092

echo "=== 3. HDFS DataNodes (Expect 2 Live) ==="
/opt/hadoop/bin/hdfs dfsadmin -report | grep "Live datanodes"
/opt/hadoop/bin/hdfs dfs -ls /datalake

echo "=== 4. YARN NodeManagers (Expect 2 RUNNING) ==="
/opt/hadoop/bin/yarn node -list
