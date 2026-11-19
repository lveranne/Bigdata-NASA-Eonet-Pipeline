# 2-VM Big Data Cluster Setup (`master` + `worker`)

Reproducible infrastructure configs, startup scripts, and verification notebook for the **NASA EONET v3 + NASA POWER** streaming pipeline.

## Cluster Topology
| Hostname | Static IP | Specs | Roles |
| :--- | :--- | :--- | :--- |
| `master` | `192.168.56.20` | 4 vCPU, 10 GB RAM, 50 GB Disk | HDFS NameNode, YARN ResourceManager, DataNode 1, NodeManager 1, Kafka 4.4.0 (KRaft), Python Producer |
| `worker` | `192.168.56.21` | 4 vCPU, 10 GB RAM, 50 GB Disk | DataNode 2, NodeManager 2, PostgreSQL 16 + TimescaleDB, Grafana |

## Quick Reproduction
1. Configure Netplan (`setup/network/netplan-*.yaml`), `/etc/hosts` (`setup/network/hosts.template`), `chrony` UTC sync, and passwordless SSH from `master` to `master` and `worker`.
2. Install OpenJDK 17, Python 3.10+, Kafka 4.4.0, Hadoop 3.4.3, and Spark on both nodes (`setup/network/bigdata-env.sh`).
3. Deploy configs from `setup/kafka/`, `setup/hadoop/`, `setup/spark/`, and `setup/systemd/`.
4. Copy `.env.example` to `.env` locally on `master` (never commit `.env`), install `requirements.txt` in `venv/`, and run `./setup/scripts/start_cluster.sh`.
5. Initialize TimescaleDB on `worker` using `setup/sql/01_init_timescaledb.sql` and start `grafana-server`.
