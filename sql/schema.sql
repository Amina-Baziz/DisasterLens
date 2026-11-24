-- DisasterLens warehouse: part 1 (schemas, dimensions, fact table, staging, ops).
-- Safe to run more than once: every statement uses IF NOT EXISTS.

-- =====================================================================
-- Schemas
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS staging;  -- temporary, rebuilt on every run
CREATE SCHEMA IF NOT EXISTS dw;       -- star schema (dimensions + fact)
CREATE SCHEMA IF NOT EXISTS mart;     -- views the dashboard reads
CREATE SCHEMA IF NOT EXISTS ops;      -- run log and data quality results


-- =====================================================================
-- Dimensions (lookup tables, labels are stored once)
-- =====================================================================

-- One row per calendar day. date_key looks like 20261114.
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      integer  PRIMARY KEY,
    full_date     date     NOT NULL UNIQUE,
    year          smallint NOT NULL,
    quarter       smallint NOT NULL,
    month         smallint NOT NULL,
    month_name    text     NOT NULL,
    week          smallint NOT NULL,
    day           smallint NOT NULL,
    weekday       smallint NOT NULL,   -- 1 = Monday ... 7 = Sunday
    weekday_name  text     NOT NULL,
    is_weekend    boolean  NOT NULL
);

-- One row per data source (usgs, eonet).
CREATE TABLE IF NOT EXISTS dw.dim_source (
    source_key   smallint PRIMARY KEY,
    source_name  text     NOT NULL UNIQUE,
    source_kind  text     NOT NULL CHECK (source_kind IN ('observation', 'curated_report')),
    description  text
);

-- One row per hazard type (earthquake, wildfire, flood, ...).
CREATE TABLE IF NOT EXISTS dw.dim_hazard_type (
    hazard_key    smallint PRIMARY KEY,
    hazard_type   text     NOT NULL UNIQUE,
    hazard_group  text     NOT NULL,
    display_name  text     NOT NULL
);


-- =====================================================================
-- Fact table: one row per source event, holding its latest known version
-- =====================================================================
CREATE TABLE IF NOT EXISTS dw.fact_event (
    event_key          text PRIMARY KEY,   -- source + ':' + source_event_id
    source_key         smallint NOT NULL REFERENCES dw.dim_source (source_key),
    hazard_key         smallint NOT NULL REFERENCES dw.dim_hazard_type (hazard_key),
    date_key           integer  NOT NULL REFERENCES dw.dim_date (date_key),
    source_event_id    text     NOT NULL,  -- the agency's own id
    source_category    text,               -- original USGS type / EONET category
    title              text,
    event_time_utc     timestamp NOT NULL,
    latitude           double precision,
    longitude          double precision,
    magnitude          double precision,   -- USGS only
    depth_km           double precision,   -- USGS only
    event_status       text CHECK (event_status IN ('open', 'closed')),  -- EONET only
    has_valid_point    boolean NOT NULL DEFAULT false,
    source_updated_utc timestamp,
    first_seen_utc     timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_seen_utc      timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    raw_object_path    text,               -- the MinIO file the row came from
    run_id             text,               -- the pipeline run that wrote it
    CONSTRAINT ck_latitude_range  CHECK (latitude  BETWEEN -90 AND 90),
    CONSTRAINT ck_longitude_range CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT ck_point_pair      CHECK ((latitude IS NULL) = (longitude IS NULL))
);

-- Indexes that match how the dashboard filters.
CREATE INDEX IF NOT EXISTS ix_fact_event_time   ON dw.fact_event (event_time_utc);
CREATE INDEX IF NOT EXISTS ix_fact_hazard_date  ON dw.fact_event (hazard_key, date_key);
CREATE INDEX IF NOT EXISTS ix_fact_source_date  ON dw.fact_event (source_key, date_key);


-- =====================================================================
-- Staging: where Spark writes clean rows before they are merged
-- =====================================================================
CREATE TABLE IF NOT EXISTS staging.stg_event (
    event_key          text PRIMARY KEY,
    source             text NOT NULL,
    source_event_id    text NOT NULL,
    hazard_type        text NOT NULL,
    source_category    text,
    title              text,
    event_time_utc     timestamp NOT NULL,
    latitude           double precision,
    longitude          double precision,
    magnitude          double precision,
    depth_km           double precision,
    event_status       text,
    source_updated_utc timestamp,
    has_valid_point    boolean NOT NULL,
    geometry_count     integer,
    raw_object_path    text,
    run_id             text,
    ingested_at_utc    timestamp
);


-- =====================================================================
-- Ops: run log and data quality results
-- =====================================================================
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id            text PRIMARY KEY,
    dag_id            text,
    status            text NOT NULL CHECK (status IN ('success', 'failed')),
    started_at_utc    timestamp,
    finished_at_utc   timestamp,
    duration_seconds  double precision,
    rows_fetched      integer,
    rows_inserted     integer,
    rows_updated      integer,
    rows_rejected     integer,
    error_message     text
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    id              bigserial PRIMARY KEY,
    run_id          text NOT NULL,
    rule_name       text NOT NULL,
    severity        text NOT NULL CHECK (severity IN ('error', 'warning')),
    passed          boolean NOT NULL,
    detail          text,
    checked_at_utc  timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);




-- =====================================================================
-- Part 2: seed data (ON CONFLICT DO NOTHING makes it safe to run twice)
-- =====================================================================

-- Sources
INSERT INTO dw.dim_source (source_key, source_name, source_kind, description) VALUES
    (1, 'usgs',  'observation',    'USGS earthquake catalog: instrument measurements'),
    (2, 'eonet', 'curated_report', 'NASA EONET: natural events curated from news and satellite sources')
ON CONFLICT (source_key) DO NOTHING;

-- Hazard types (same list as the data contract)
INSERT INTO dw.dim_hazard_type (hazard_key, hazard_type, hazard_group, display_name) VALUES
    (1,  'earthquake',          'geophysical',    'Earthquake'),
    (2,  'volcano',             'geophysical',    'Volcano'),
    (3,  'landslide',           'geophysical',    'Landslide'),
    (4,  'wildfire',            'climatological', 'Wildfire'),
    (5,  'drought',             'climatological', 'Drought'),
    (6,  'temperature_extreme', 'climatological', 'Temperature extreme'),
    (7,  'severe_storm',        'meteorological', 'Severe storm'),
    (8,  'dust_haze',           'meteorological', 'Dust and haze'),
    (9,  'snow',                'meteorological', 'Snow'),
    (10, 'flood',               'hydrological',   'Flood'),
    (11, 'sea_lake_ice',        'cryospheric',    'Sea and lake ice'),
    (12, 'water_color',         'other',          'Water color'),
    (13, 'other',               'other',          'Other')
ON CONFLICT (hazard_key) DO NOTHING;

-- Calendar: one row per day from 2020 to 2030
INSERT INTO dw.dim_date
    (date_key, full_date, year, quarter, month, month_name, week, day, weekday, weekday_name, is_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::integer,
    d::date,
    extract(year    FROM d)::smallint,
    extract(quarter FROM d)::smallint,
    extract(month   FROM d)::smallint,
    trim(to_char(d, 'Month')),
    extract(week    FROM d)::smallint,
    extract(day     FROM d)::smallint,
    extract(isodow  FROM d)::smallint,
    trim(to_char(d, 'Day')),
    extract(isodow  FROM d) IN (6, 7)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;


-- =====================================================================
-- Part 3: marts (what the dashboard reads)
-- =====================================================================

-- Events per day, source and hazard type. Stored on disk and refreshed hourly,
-- because the dashboard reads it on every page load but the data changes hourly.
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_daily_hazard_summary AS
SELECT
    d.full_date,
    s.source_name,
    h.hazard_type,
    h.hazard_group,
    h.display_name,
    count(*)           AS event_count,
    avg(f.magnitude)   AS avg_magnitude,   -- earthquakes only, empty otherwise
    max(f.magnitude)   AS max_magnitude
FROM dw.fact_event f
JOIN dw.dim_date        d ON d.date_key   = f.date_key
JOIN dw.dim_source      s ON s.source_key = f.source_key
JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
GROUP BY d.full_date, s.source_name, h.hazard_type, h.hazard_group, h.display_name;

-- Needed so the view can be refreshed without blocking the dashboard.
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_daily_hazard_summary
    ON mart.mv_daily_hazard_summary (full_date, source_name, hazard_type);

-- Events that have a usable location, with names already joined.
CREATE OR REPLACE VIEW mart.v_event_map AS
SELECT
    f.event_key,
    f.title,
    f.event_time_utc,
    f.latitude,
    f.longitude,
    f.magnitude,
    f.event_status,
    s.source_name,
    h.hazard_type,
    h.hazard_group,
    h.display_name
FROM dw.fact_event f
JOIN dw.dim_source      s ON s.source_key = f.source_key
JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
WHERE f.has_valid_point;

-- The 200 most recent events.
CREATE OR REPLACE VIEW mart.v_latest_events AS
SELECT
    f.event_key,
    f.title,
    f.event_time_utc,
    f.magnitude,
    f.event_status,
    f.has_valid_point,
    s.source_name,
    h.display_name AS hazard
FROM dw.fact_event f
JOIN dw.dim_source      s ON s.source_key = f.source_key
JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
ORDER BY f.event_time_utc DESC
LIMIT 200;

-- Every event with its labels joined. The dashboard loads this once and filters it
-- (magnitude, depth, region), which the daily summary cannot do.
CREATE OR REPLACE VIEW mart.v_events AS
SELECT
    f.event_key,
    f.title,
    f.event_time_utc,
    f.latitude,
    f.longitude,
    f.magnitude,
    f.depth_km,
    f.event_status,
    f.has_valid_point,
    s.source_name,
    h.hazard_type,
    h.hazard_group,
    h.display_name
FROM dw.fact_event f
JOIN dw.dim_source      s ON s.source_key = f.source_key
JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key;

-- Per source: how many events, how many have a location, and how fresh the data is.
CREATE OR REPLACE VIEW mart.v_source_coverage AS
SELECT
    s.source_name,
    count(f.event_key)                           AS total_events,
    count(f.event_key) FILTER (WHERE f.has_valid_point) AS events_with_location,
    min(f.event_time_utc)                        AS earliest_event_utc,
    max(f.event_time_utc)                        AS latest_event_utc,
    max(f.last_seen_utc)                         AS last_seen_utc
FROM dw.dim_source s
LEFT JOIN dw.fact_event f ON f.source_key = s.source_key
GROUP BY s.source_name;


-- =====================================================================
-- Part 4: roles (least privilege)
-- Login passwords are set later from .env, never written in this file.
-- =====================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        CREATE ROLE pipeline_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
        CREATE ROLE dashboard_reader NOLOGIN;
    END IF;
END
$$;

-- The pipeline can read and write staging, the warehouse and the run log.
GRANT USAGE ON SCHEMA staging, dw, mart, ops TO pipeline_writer;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA staging, dw, ops TO pipeline_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO pipeline_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO pipeline_writer;

-- The dashboard can only read the marts and the run log.
GRANT USAGE ON SCHEMA mart, ops TO dashboard_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mart, ops TO dashboard_reader;
