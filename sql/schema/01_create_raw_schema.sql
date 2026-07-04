-- =============================================================================
-- RetailFlow Pipeline — Schema: raw
-- =============================================================================
-- This script creates the `raw` schema and its tables.
-- The raw layer is the first stop for CSV data loaded into PostgreSQL.
-- Tables here mirror the CSV structure exactly (no transformations).
--
-- Run this script:
--   psql -d retailflow -f sql/schema/01_create_raw_schema.sql
-- =============================================================================

-- Create the raw schema
CREATE SCHEMA IF NOT EXISTS raw;

-- Drop existing tables for idempotency (safe to re-run)
DROP TABLE IF EXISTS raw.orders CASCADE;
DROP TABLE IF EXISTS raw.customers CASCADE;
DROP TABLE IF EXISTS raw.products CASCADE;

-- Raw customers table
CREATE TABLE raw.customers (
    customer_id    TEXT PRIMARY KEY,
    first_name     TEXT NOT NULL,
    last_name      TEXT NOT NULL,
    email          TEXT NOT NULL,
    country        TEXT,
    city           TEXT,
    signup_date    TEXT,  -- Stored as TEXT because date format may vary
    age            INTEGER,
    gender         TEXT
);

-- Raw products table
CREATE TABLE raw.products (
    product_id      TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    price_cents     INTEGER NOT NULL CHECK (price_cents > 0),
    stock_quantity  INTEGER DEFAULT 0,
    supplier_country TEXT
);

-- Raw orders table
CREATE TABLE raw.orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL,
    product_id    TEXT NOT NULL,
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    order_date    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('completed', 'returned', 'pending')),
    discount_pct  INTEGER NOT NULL CHECK (discount_pct >= 0 AND discount_pct <= 50),
    shipping_days INTEGER NOT NULL CHECK (shipping_days > 0)
);
