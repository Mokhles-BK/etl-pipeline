-- Warehouse (star schema) for the earthquake ETL pipeline.
-- Fed from staging.earthquakes by scripts/etl/warehouse.py — never written to
-- directly. Idempotent: dimensions use ON CONFLICT DO NOTHING (immutable once
-- created), the fact table uses ON CONFLICT (event_id) DO UPDATE, matching the
-- same upsert convention as staging_schema.sql.

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ---------------------------------------------------------------------------
-- dim_time — one row per calendar date that appears in the source data.
-- date_key is YYYYMMDD as an integer (e.g. 20260923), the standard warehouse
-- convention: sorts correctly, joins on a plain integer, no timezone ambiguity
-- once the date component is extracted.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    date_key      INTEGER PRIMARY KEY,
    full_date     DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    day           SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,   -- 0=Sunday .. 6=Saturday (Postgres DOW)
    month_name    TEXT NOT NULL
);

-- ---------------------------------------------------------------------------
-- dim_location — coarse geographic bucket, not one row per exact coordinate.
-- Region is parsed from the USGS `place` string (text after the last comma,
-- e.g. "Indonesia" from "126 km NNE of Teluknaga, Indonesia"); lat/lon are
-- floored to whole degrees so nearby events group into the same bucket rather
-- than never matching due to float precision. Natural key: the triple below.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_key  BIGSERIAL PRIMARY KEY,
    region        TEXT NOT NULL,
    lat_bucket    INTEGER,
    lon_bucket    INTEGER,
    UNIQUE (region, lat_bucket, lon_bucket)
);

-- ---------------------------------------------------------------------------
-- dim_magnitude_type — the small set of USGS magnitude scales (mww, mb, ml...)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_magnitude_type (
    mag_type_key  SERIAL PRIMARY KEY,
    mag_type      TEXT NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------
-- fact_earthquake_events — one row per event, at the same grain as
-- staging.earthquakes. event_id is kept as the natural key so a rerun of the
-- transform upserts in place rather than duplicating.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.fact_earthquake_events (
    event_id      TEXT PRIMARY KEY,
    date_key      INTEGER NOT NULL REFERENCES warehouse.dim_time (date_key),
    location_key  BIGINT REFERENCES warehouse.dim_location (location_key),
    mag_type_key  INTEGER REFERENCES warehouse.dim_magnitude_type (mag_type_key),
    mag           DOUBLE PRECISION NOT NULL,
    depth         DOUBLE PRECISION,
    sig           DOUBLE PRECISION,
    felt          DOUBLE PRECISION,
    nst           INTEGER,
    gap           DOUBLE PRECISION,
    dmin          DOUBLE PRECISION,
    rms           DOUBLE PRECISION,
    tsunami       DOUBLE PRECISION,
    event_time    BIGINT NOT NULL,   -- epoch ms, kept for exact ordering/joins back to staging
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for the query patterns a dashboard actually needs: filter/aggregate
-- by date and by magnitude, and join out to each dimension.
CREATE INDEX IF NOT EXISTS idx_fact_earthquakes_date_key ON warehouse.fact_earthquake_events (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_earthquakes_location_key ON warehouse.fact_earthquake_events (location_key);
CREATE INDEX IF NOT EXISTS idx_fact_earthquakes_mag_type_key ON warehouse.fact_earthquake_events (mag_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_earthquakes_mag ON warehouse.fact_earthquake_events (mag);
