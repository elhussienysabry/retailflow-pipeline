-- =============================================================================
-- RetailFlow Pipeline — Schema: audit
-- =============================================================================
-- This script creates the `audit` schema and its pipeline_runs table.
-- Every pipeline orchestration run writes one row here, regardless of
-- success or failure, providing a permanent execution audit trail.
--
-- Run this script:
--   psql -d retailflow -f sql/schema/04_create_audit_schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    start_time        TIMESTAMPTZ      NOT NULL,
    end_time          TIMESTAMPTZ,
    status            VARCHAR(20)      NOT NULL DEFAULT 'RUNNING'
                      CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SCHEMA_DRIFT')),
    records_ingested  INTEGER          NOT NULL DEFAULT 0,
    records_rejected  INTEGER          NOT NULL DEFAULT 0,
    parquet_file_path TEXT,
    duration_seconds  DOUBLE PRECISION,
    sla_breached      BOOLEAN          NOT NULL DEFAULT FALSE,
    error_message     TEXT,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time
    ON audit.pipeline_runs (start_time DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON audit.pipeline_runs (status);
