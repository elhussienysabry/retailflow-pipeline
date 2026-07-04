-- =============================================================================
-- Analytics Query: Product Category Performance
-- =============================================================================
-- Business Question: Which product categories generate the most revenue,
-- and how many units are sold in each?
--
-- This query helps the inventory and procurement teams decide which
-- categories to prioritize and stock more heavily.
--
-- Usage:
--   psql -d retailflow -f sql/analytics/product_category_performance.sql
-- =============================================================================

WITH category_stats AS (
    SELECT
        p.category,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.quantity) AS total_units_sold,
        ROUND(SUM(f.net_revenue_dollars), 2) AS total_net_revenue,
        ROUND(AVG(f.discount_pct), 2) AS avg_discount_pct
    FROM marts.dim_products AS p
    INNER JOIN marts.fct_orders AS f
        ON p.product_id = f.product_id
    WHERE f.status = 'completed'
    GROUP BY p.category
)

SELECT
    category,
    total_orders,
    total_units_sold,
    total_net_revenue,
    avg_discount_pct,
    -- WHY: Show what fraction of total revenue each category represents.
    ROUND(
        total_net_revenue / SUM(total_net_revenue) OVER () * 100,
        2
    ) AS revenue_share_pct
FROM category_stats
ORDER BY total_net_revenue DESC;
