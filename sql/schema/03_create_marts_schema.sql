-- =============================================================================
-- RetailFlow Pipeline — Schema: marts
-- =============================================================================
-- The marts schema holds the final business-ready tables: dimensions and
-- fact tables in a star schema. dbt manages these tables, but we create
-- the schemas here so they exist before dbt runs.
--
-- Run this script:
--   psql -d retailflow -f sql/schema/03_create_marts_schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS intermediate;
