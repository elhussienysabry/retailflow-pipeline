-- assert_positive_revenue.sql
-- dbt data test: Ensures that no order has negative net revenue.
--
-- A negative net_revenue_cents would indicate a bug in our discount
-- calculation. This test should return zero rows.
--
-- WHY: This is a "singular" data test — a SQL query that dbt runs and
-- expects to return zero rows. If any rows come back, the test fails.

SELECT
    order_id,
    net_revenue_cents
FROM {{ ref('int_orders_enriched') }}
WHERE net_revenue_cents < 0
