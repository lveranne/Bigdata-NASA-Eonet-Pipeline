-- Run on worker: sudo -u postgres psql -f setup/sql/01_init_timescaledb.sql
CREATE USER eonet_user WITH PASSWORD 'CHANGE_ME_STRONG_PASSWORD';
CREATE DATABASE eonet_db OWNER eonet_user;
\c eonet_db
CREATE EXTENSION IF NOT EXISTS timescaledb;
GRANT ALL PRIVILEGES ON DATABASE eonet_db TO eonet_user;
