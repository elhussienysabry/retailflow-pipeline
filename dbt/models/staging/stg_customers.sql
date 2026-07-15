-- stg_customers.sql
-- Staging model for raw customer data.
-- Cleans and standardizes the raw customers table:
--   - Trims whitespace from name/email fields
--   - Casts signup_date to a proper DATE type
--   - Deduplicates by natural key (email) to survive idempotent
--     pipeline runs where the same person gets a different
--     customer_id UUID each day.

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

latest AS (
    SELECT *
    FROM source
    WHERE _execution_date = (SELECT MAX(_execution_date) FROM source)
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
        TRIM(gender) AS gender,
        _execution_date
    FROM latest
    WHERE customer_id IS NOT NULL
),

deduped AS (
    -- WHY: uuid.uuid4() generates random UUIDs per run (not seeded),
    -- so the same real customer gets a different customer_id every day.
    -- Deduplicating by email (the natural business key) keeps one row
    -- per unique person, which makes the unique email test valid.
    SELECT DISTINCT ON (email) *
    FROM cleaned
    ORDER BY email, customer_id
)

SELECT * FROM deduped
