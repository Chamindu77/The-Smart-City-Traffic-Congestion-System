-- Table 1: Every raw sensor event (stream layer writes here)
CREATE TABLE IF NOT EXISTS traffic_events (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(50)  NOT NULL,
    event_timestamp TIMESTAMP    NOT NULL,
    vehicle_count   INTEGER      NOT NULL,
    avg_speed       FLOAT        NOT NULL,
    ingested_at     TIMESTAMP    DEFAULT NOW()
);

-- Table 2: Windowed congestion index (5-min aggregates from Spark)
CREATE TABLE IF NOT EXISTS congestion_index (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(50)  NOT NULL,
    window_start    TIMESTAMP    NOT NULL,
    window_end      TIMESTAMP    NOT NULL,
    total_vehicles  INTEGER      NOT NULL,
    avg_speed       FLOAT        NOT NULL,
    congestion_idx  FLOAT        NOT NULL,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- Table 3: Critical alerts when avg_speed < 10 km/h
CREATE TABLE IF NOT EXISTS critical_traffic (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(50)  NOT NULL,
    event_timestamp TIMESTAMP    NOT NULL,
    vehicle_count   INTEGER      NOT NULL,
    avg_speed       FLOAT        NOT NULL,
    alert_message   TEXT,
    alerted_at      TIMESTAMP    DEFAULT NOW()
);

-- Table 4: Airflow nightly report output
CREATE TABLE IF NOT EXISTS daily_peak_report (
    id              SERIAL PRIMARY KEY,
    report_date     DATE         NOT NULL,
    sensor_id       VARCHAR(50)  NOT NULL,
    peak_hour       INTEGER      NOT NULL,
    peak_vehicles   INTEGER      NOT NULL,
    avg_congestion  FLOAT        NOT NULL,
    needs_police    BOOLEAN      DEFAULT FALSE,
    created_at      TIMESTAMP    DEFAULT NOW()
);