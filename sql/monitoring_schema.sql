-- Run log for pipeline monitoring. One row per load or transform run, written
-- by scripts/etl/runlog.py and read by the dashboard's "Pipeline health" page.
-- Idempotent: safe to apply repeatedly.

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id            BIGSERIAL PRIMARY KEY,
    stage             TEXT NOT NULL CHECK (stage IN ('load', 'transform')),
    source            TEXT,
    batch_id          TEXT,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL,
    duration_ms       INTEGER NOT NULL,
    ok                BOOLEAN NOT NULL,
    records_fetched   INTEGER NOT NULL DEFAULT 0,
    records_loaded    INTEGER NOT NULL DEFAULT 0,
    records_rejected  INTEGER NOT NULL DEFAULT 0,
    error             TEXT
);

-- The health page always asks "what happened recently?"
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON monitoring.pipeline_runs (started_at DESC);
