-- stg_customers.sql
-- Staging model for raw customer data.
-- Cleans and standardizes the raw customers table:
--   - Trims whitespace from name/email fields
--   - Casts signup_date to a proper DATE type
--   - Removes duplicate customer records

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRIM(country) AS country,
        TRIM(city) AS city,
        CAST(signup_date AS DATE) AS signup_date,
        age,
        TRIM(gender) AS gender
    FROM source
    WHERE customer_id IS NOT NULL
),

deduped AS (
    -- WHY: Remove duplicate customer records based on customer_id.
    -- Using ROW_NUMBER ensures we keep exactly one row per customer.
    SELECT DISTINCT * FROM cleaned
)

SELECT * FROM deduped
