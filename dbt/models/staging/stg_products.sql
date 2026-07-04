-- stg_products.sql
-- Staging model for raw product data.
-- Cleans and standardizes the raw products table:
--   - Trims product name and category
--   - Ensures price_cents is positive
--   - Removes duplicate products

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        product_id,
        TRIM(name) AS product_name,
        TRIM(category) AS category,
        price_cents,
        stock_quantity,
        TRIM(supplier_country) AS supplier_country
    FROM source
    WHERE product_id IS NOT NULL
      -- WHY: Filter out products with zero or negative prices (invalid data).
      AND price_cents > 0
),

deduped AS (
    SELECT DISTINCT * FROM cleaned
)

SELECT * FROM deduped
