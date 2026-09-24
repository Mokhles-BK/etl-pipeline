-- Staging schema for the ETL pipeline.
-- Idempotent: CREATE TABLE IF NOT EXISTS + ON CONFLICT (natural key) DO UPDATE
-- means re-running the load never duplicates rows.

CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- earthquakes  (USGS FDSNWS event feed, GeoJSON features)
-- Natural key: the event id (e.g. "us6000twys"). The incremental cursor is the
-- event `time` (epoch ms), stored in staging.load_cursors.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.earthquakes (
    event_id      TEXT PRIMARY KEY,
    mag           DOUBLE PRECISION NOT NULL,
    mag_type      TEXT,
    place         TEXT,
    event_time    BIGINT NOT NULL,
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    depth         DOUBLE PRECISION,
    status        TEXT,
    sig           DOUBLE PRECISION,
    net           TEXT,
    nst           INTEGER,
    tsunami       DOUBLE PRECISION,
    alert        TEXT,
    cdi           DOUBLE PRECISION,
    mmi           DOUBLE PRECISION,
    gap           DOUBLE PRECISION,
    dmin          DOUBLE PRECISION,
    rms           DOUBLE PRECISION,
    felt          DOUBLE PRECISION,
    event_type    TEXT,
    title         TEXT,
    event_url     TEXT,
    detail_url    TEXT,
    updated       BIGINT,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id      UUID
);

-- ---------------------------------------------------------------------------
-- load_cursors  — durable incremental cursor, one row per entity.
-- The scheduler reads the cursor value per entity here so a run only fetches
-- records newer than what was last loaded. Kept in staging so it survives
-- restarts and is independent of the scheduler's own state.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.load_cursors (
    entity        TEXT PRIMARY KEY,
    last_seen_id  BIGINT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO staging.load_cursors (entity, last_seen_id)
VALUES ('earthquakes', 0)
ON CONFLICT (entity) DO UPDATE SET last_seen_id = staging.load_cursors.last_seen_id;

-- ---------------------------------------------------------------------------
-- load_errors  — bad rows rejected by source validation, routed here instead
-- of crashing the load. The natural key is (entity, source_id) so a rerun
-- that re-rejects the same bad row does not duplicate the error row.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.load_errors (
    id            BIGSERIAL PRIMARY KEY,
    entity        TEXT NOT NULL,
    source_id     TEXT,
    error         TEXT NOT NULL,
    raw           JSONB,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id      UUID,
    UNIQUE (entity, source_id)  -- idempotent on (entity, source_id)
);

-- Ensure the unique index exists (in case table was created with a different constraint)
CREATE UNIQUE INDEX IF NOT EXISTS load_errors_entity_source_id_key
ON staging.load_errors (entity, source_id);

-- Fast lookup for "what has been loaded so far" / reconciliation.
CREATE INDEX IF NOT EXISTS idx_earthquakes_event_time ON staging.earthquakes (event_time);
CREATE INDEX IF NOT EXISTS idx_earthquakes_mag ON staging.earthquakes (mag);
CREATE INDEX IF NOT EXISTS idx_earthquakes_loaded_at ON staging.earthquakes (loaded_at);