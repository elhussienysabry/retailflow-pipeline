-- =============================================================================
-- Analytics Query: Monthly Sales Trend
-- =============================================================================
-- Business Question: How is our monthly net revenue trending over time?
--
-- This query helps executives and investors understand revenue patterns,
-- seasonality, and growth trends.
--
-- Usage:
--   psql -d retailflow -f sql/analytics/monthly_sales_trend.sql
-- =============================================================================

WITH monthly_revenue AS (
    -- WHY: Truncate order_date to month for grouping.
    SELECT
        DATE_TRUNC('month', order_date)::DATE AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(net_revenue_dollars), 2) AS net_revenue
    FROM marts.fct_orders
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('month', order_date)
)

SELECT
    month,
    total_orders,
    net_revenue,
    -- WHY: LAG computes the previous month's revenue for comparison.
    ROUND(
        (net_revenue - LAG(net_revenue) OVER (ORDER BY month))
        / NULLIF(LAG(net_revenue) OVER (ORDER BY month), 0) * 100,
        2
    ) AS month_over_month_growth_pct
FROM monthly_revenue
ORDER BY month;
