-- stg_products.sql
-- Staging model for raw product data.
-- Cleans and standardizes the raw products table:
--   - Trims product name and category
--   - Ensures price_cents is positive
--   - Filters to the latest _execution_date to eliminate duplicates
--     from idempotent pipeline runs across calendar days.

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

latest AS (
    SELECT *
    FROM source
    WHERE _execution_date = (SELECT MAX(_execution_date) FROM source)
),

cleaned AS (
    SELECT
        product_id,
        TRIM(name) AS product_name,
        TRIM(category) AS category,
        price_cents,
        stock_quantity,
        TRIM(supplier_country) AS supplier_country
    FROM latest
    WHERE product_id IS NOT NULL
      -- WHY: Filter out products with zero or negative prices (invalid data).
      AND price_cents > 0
),

deduped AS (
    SELECT DISTINCT * FROM cleaned
)

SELECT * FROM deduped
