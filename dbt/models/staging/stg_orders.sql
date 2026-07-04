-- stg_orders.sql
-- Staging model for raw orders data.
-- Cleans and standardizes the raw orders table:
--   - Casts string dates to proper date types
--   - Filters out rows with null customer or product references
--   - Renames columns for clarity

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        -- Primary key
        order_id,

        -- Foreign keys
        customer_id,
        product_id,

        -- Measures
        quantity,
        discount_pct,

        -- Date handling: cast ISO date string to proper date type
        -- WHY: Raw CSV stores dates as strings; dbt ensures proper DATE type.
        CAST(order_date AS DATE) AS order_date,

        -- Status normalization: trim whitespace and lowercase
        LOWER(TRIM(status)) AS status,

        -- Shipping metadata
        shipping_days

    FROM source
    -- WHY: Remove orphaned records that can't be linked to customers or products.
    WHERE customer_id IS NOT NULL
      AND product_id IS NOT NULL
      AND order_id IS NOT NULL
),

deduped AS (
    -- WHY: Remove exact duplicate rows if the raw data has any.
    SELECT DISTINCT * FROM cleaned
)

SELECT * FROM deduped
