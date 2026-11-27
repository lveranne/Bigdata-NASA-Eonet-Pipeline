CREATE TABLE IF NOT EXISTS eonet_events_stream (
    record_id          TEXT NOT NULL,
    event_id           TEXT NOT NULL,
    title              TEXT,
    category_id        TEXT,
    source_id          TEXT,
    status             TEXT,
    event_time_utc     TIMESTAMPTZ NOT NULL,
    first_seen_utc     TIMESTAMPTZ,
    closed_time_utc    TIMESTAMPTZ,
    ingested_at_utc    TIMESTAMPTZ,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    magnitude_value    DOUBLE PRECISION,
    magnitude_unit     TEXT,
    temp_2m_c          DOUBLE PRECISION,
    humidity_2m_pct    DOUBLE PRECISION,
    wind_10m_ms        DOUBLE PRECISION,
    precip_mm_day      DOUBLE PRECISION,
    data_split         TEXT DEFAULT 'test',
    PRIMARY KEY (record_id, event_time_utc)
);
SELECT create_hypertable('eonet_events_stream', 'event_time_utc', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS eonet_predictions (
    event_id              TEXT NOT NULL,
    model_version         TEXT NOT NULL,
    predicted_at_utc      TIMESTAMPTZ NOT NULL,
    category_id           TEXT,
    status                TEXT,
    elapsed_hours         DOUBLE PRECISION,
    pred_duration_hours   DOUBLE PRECISION,
    pred_remaining_hours  DOUBLE PRECISION,
    pred_severity_tier    INTEGER,
    actual_duration_hours DOUBLE PRECISION,
    abs_error_hours       DOUBLE PRECISION,
    closed_time_utc       TIMESTAMPTZ,
    data_split            TEXT DEFAULT 'test',
    PRIMARY KEY (event_id, model_version, predicted_at_utc)
);
SELECT create_hypertable('eonet_predictions', 'predicted_at_utc', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS eonet_prediction_history (
    record_id             TEXT NOT NULL,
    event_id              TEXT NOT NULL,
    model_version         TEXT NOT NULL,
    scored_at_utc         TIMESTAMPTZ NOT NULL,
    category_id           TEXT,
    status                TEXT,
    pred_duration_hours   DOUBLE PRECISION,
    actual_duration_hours DOUBLE PRECISION,
    abs_error_hours       DOUBLE PRECISION,
    squared_error_hours   DOUBLE PRECISION,
    pred_severity_tier    INTEGER,
    actual_severity_tier  INTEGER,
    data_split            TEXT DEFAULT 'test',
    PRIMARY KEY (record_id, model_version, scored_at_utc)
);
SELECT create_hypertable('eonet_prediction_history', 'scored_at_utc', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS eonet_lifecycle_events (
    event_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    component       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    model_version   TEXT,
    details         JSONB,
    PRIMARY KEY (event_ts, component, event_type)
);
SELECT create_hypertable('eonet_lifecycle_events', 'event_ts', if_not_exists => TRUE);
