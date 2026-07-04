-- dim_products.sql
-- Product dimension table for the star schema.
-- One row per product with attributes and pre-computed sales metrics.

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

-- WHY: Pre-compute aggregate sales metrics per product for analysis.
product_metrics AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.price_cents,
        p.stock_quantity,
        p.supplier_country,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(o.quantity), 0) AS total_units_sold,
        COALESCE(SUM(o.quantity * p.price_cents), 0) AS total_revenue_cents
    FROM products AS p
    LEFT JOIN {{ ref('stg_orders') }} AS o ON p.product_id = o.product_id
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM product_metrics
