-- int_orders_enriched.sql
-- Intermediate model that enriches order data with customer and product details.
-- This is the "join" layer that brings staging tables together.
--
-- Business value:
--   Combines orders + customers + products into a single denormalized view.
--   All downstream mart models read from this view instead of re-joining.

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

enriched AS (
    SELECT
        -- Order fields
        o.order_id,
        o.order_date,
        o.quantity,
        o.discount_pct,
        o.status,
        o.shipping_days,

        -- Customer fields
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_full_name,
        c.email AS customer_email,
        c.country AS customer_country,
        c.city AS customer_city,
        c.age AS customer_age,
        c.gender AS customer_gender,
        c.signup_date AS customer_signup_date,

        -- Product fields
        p.product_id,
        p.product_name,
        p.category AS product_category,
        p.price_cents,
        p.stock_quantity,
        p.supplier_country AS product_supplier_country,

        -- Calculated measures
        -- WHY: Compute line-item total in cents (quantity * unit price),
        -- then apply the discount percentage.
        o.quantity * p.price_cents AS gross_revenue_cents,
        ROUND(
            o.quantity * p.price_cents * (1.0 - o.discount_pct / 100.0)
        ) AS net_revenue_cents

    FROM orders AS o
    -- WHY: LEFT JOIN customers and products so we don't lose orders even if
    -- the customer or product was filtered out in staging.
    LEFT JOIN customers AS c ON o.customer_id = c.customer_id
    LEFT JOIN products AS p ON o.product_id = p.product_id
)

SELECT * FROM enriched
