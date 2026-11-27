# Bigdata-NASA-Eonet-Pipeline

End-to-End Streaming & Batch Lakehouse Pipeline on NASA EONET v3 + NASA POWER API.

## Cluster Topology (2-VM VirtualBox Cluster)
- **master**: HDFS NameNode, HDFS DataNode, YARN ResourceManager, YARN NodeManager, Kafka 4.0.0 (KRaft), Python Producer
- **worker**: HDFS DataNode, YARN NodeManager, PostgreSQL 16 + TimescaleDB, Grafana

## Pinned Versions
- Ubuntu Server 22.04 LTS | OpenJDK 17
- Apache Hadoop 3.4.1 (HDFS Replication Factor: 2)
- Apache Spark 3.5.3 (on YARN)
- Apache Kafka 4.0.0 (KRaft mode)
- PostgreSQL 16 + TimescaleDB 2.x | Grafana OSS
