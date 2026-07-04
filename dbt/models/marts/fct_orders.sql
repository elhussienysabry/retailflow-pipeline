-- fct_orders.sql
-- Fact table for orders in the star schema.
-- One row per order with measures (revenue in dollars) and foreign keys
-- to the dimension tables.

WITH enriched AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

-- WHY: Use the cents_to_dollars macro to convert cents to dollars for
-- business readability. Analysts prefer dollars, not cents.
converted AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        discount_pct,
        status,
        shipping_days,
        {{ cents_to_dollars('gross_revenue_cents') }} AS gross_revenue_dollars,
        {{ cents_to_dollars('net_revenue_cents') }} AS net_revenue_dollars
    FROM enriched
)

SELECT * FROM converted
