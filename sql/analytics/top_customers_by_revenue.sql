-- =============================================================================
-- Analytics Query: Top Customers by Revenue
-- =============================================================================
-- Business Question: Who are our top 10 highest-value customers by total
-- net revenue?
--
-- This query helps the marketing team identify VIP customers for loyalty
-- programs and targeted promotions.
--
-- Usage:
--   psql -d retailflow -f sql/analytics/top_customers_by_revenue.sql
-- =============================================================================

WITH customer_revenue AS (
    -- WHY: CTE ensures we compute revenue once and reuse it.
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.city,
        SUM(f.net_revenue_dollars) AS total_net_revenue
    FROM marts.dim_customers AS c
    INNER JOIN marts.fct_orders AS f
        ON c.customer_id = f.customer_id
    WHERE f.status = 'completed'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.country, c.city
)

SELECT
    first_name,
    last_name,
    email,
    country,
    city,
    ROUND(total_net_revenue, 2) AS total_net_revenue
FROM customer_revenue
ORDER BY total_net_revenue DESC
LIMIT 10;
