-- assert_no_null_customer_id.sql
-- dbt data test: Ensures that every order has a valid customer_id.
--
-- Null customer IDs would break downstream joins to dim_customers.
-- This test should return zero rows.
--
-- WHY: A singular test that verifies referential integrity at the
-- staging layer. Catches data quality issues early.

SELECT
    order_id,
    customer_id
FROM {{ ref('stg_orders') }}
WHERE customer_id IS NULL
