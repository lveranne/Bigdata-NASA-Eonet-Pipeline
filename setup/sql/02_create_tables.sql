\c eonet_db

-- 1. Main streaming observations hypertable
CREATE TABLE IF NOT EXISTS eonet_events_stream (
    record_id         TEXT        NOT NULL,
    event_id          TEXT        NOT NULL,
    title             TEXT,
    category_id       TEXT        NOT NULL,
    source_id         TEXT,
    status            TEXT        NOT NULL,
    event_time_utc    TIMESTAMPTZ NOT NULL,
    first_seen_utc    TIMESTAMPTZ,
    closed_time_utc   TIMESTAMPTZ,
    ingested_at_utc   TIMESTAMPTZ NOT NULL,
    longitude         DOUBLE PRECISION,
    latitude          DOUBLE PRECISION,
    magnitude_value   DOUBLE PRECISION,
    magnitude_unit    TEXT,
    temp_2m_c         DOUBLE PRECISION,
    humidity_2m_pct   DOUBLE PRECISION,
    wind_10m_ms       DOUBLE PRECISION,
    precip_mm_day     DOUBLE PRECISION,
    PRIMARY KEY (record_id, event_time_utc)
);

SELECT create_hypertable(
    'eonet_events_stream',
    'event_time_utc',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_eonet_event_id ON eonet_events_stream (event_id, event_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_eonet_category ON eonet_events_stream (category_id, event_time_utc DESC);

-- 2. ML predictions table (populated in the next phase)
CREATE TABLE IF NOT EXISTS eonet_predictions (
    event_id              TEXT PRIMARY KEY,
    category_id           TEXT,
    status                TEXT,
    predicted_duration_hr DOUBLE PRECISION,
    predicted_severity    INTEGER,
    severity_label        TEXT,
    scored_at_utc         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO eonet_user;
