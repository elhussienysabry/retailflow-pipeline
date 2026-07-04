-- =============================================================================
-- Analytics Query: Customer Cohort Analysis
-- =============================================================================
-- Business Question: How does customer spending behavior change over time
-- based on when they first signed up (cohort)?
--
-- Cohort analysis helps the marketing team understand retention and
-- whether newer customers spend more or less than older ones.
--
-- Usage:
--   psql -d retailflow -f sql/analytics/customer_cohort_analysis.sql
-- =============================================================================

WITH customer_cohorts AS (
    -- WHY: Define each customer's cohort as the month they signed up.
    SELECT
        customer_id,
        DATE_TRUNC('month', signup_date)::DATE AS cohort_month
    FROM marts.dim_customers
),

customer_orders AS (
    -- WHY: Join orders to customers and compute the cohort-relative month.
    SELECT
        cc.cohort_month,
        f.order_date,
        f.net_revenue_dollars,
        -- WHY: The "cohort index" — how many months since signup.
        EXTRACT(MONTH FROM AGE(f.order_date, cc.cohort_month))::INTEGER
            AS cohort_index
    FROM marts.fct_orders AS f
    INNER JOIN customer_cohorts AS cc
        ON f.customer_id = cc.customer_id
    WHERE f.status = 'completed'
),

cohort_aggregated AS (
    SELECT
        cohort_month,
        cohort_index,
        COUNT(DISTINCT customer_id) AS active_customers,
        ROUND(SUM(net_revenue_dollars), 2) AS total_revenue,
        ROUND(AVG(net_revenue_dollars), 2) AS avg_revenue_per_customer
    FROM customer_orders
    GROUP BY cohort_month, cohort_index
)

SELECT
    cohort_month,
    cohort_index,
    active_customers,
    total_revenue,
    avg_revenue_per_customer
FROM cohort_aggregated
ORDER BY cohort_month, cohort_index;
