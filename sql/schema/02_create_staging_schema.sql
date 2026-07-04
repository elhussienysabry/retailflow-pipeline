-- =============================================================================
-- RetailFlow Pipeline — Schema: staging
-- =============================================================================
-- The staging schema holds cleaned, typed views of the raw data.
-- dbt manages these tables/views, but we create the schema here so it
-- exists before dbt runs.
--
-- Run this script:
--   psql -d retailflow -f sql/schema/02_create_staging_schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
