-- dim_customers.sql
-- Customer dimension table for the star schema.
-- One row per customer with demographic attributes.
-- Sourced from the cleaned staging model.

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- WHY: Compute aggregate metrics per customer for analyst convenience.
customer_metrics AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.city,
        c.signup_date,
        c.age,
        c.gender,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(o.quantity * p.price_cents), 0) AS lifetime_value_cents
    FROM customers AS c
    LEFT JOIN {{ ref('stg_orders') }} AS o ON c.customer_id = o.customer_id
    LEFT JOIN {{ ref('stg_products') }} AS p ON o.product_id = p.product_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT * FROM customer_metrics
