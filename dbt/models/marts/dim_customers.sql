WITH scd_customers AS (
    SELECT * FROM {{ ref('scd_customers') }}
),

customer_metrics AS (
    SELECT
        c.dbt_scd_id AS customer_key,
        c.customer_id,
        {% if var('mask_pii', false) %}
            {{ mask_string('c.first_name') }} AS first_name,
            {{ mask_string('c.last_name') }} AS last_name,
            {{ mask_email('c.email') }} AS email,
        {% else %}
            c.first_name,
            c.last_name,
            c.email,
        {% endif %}
        c.country,
        c.city,
        c.signup_date,
        c.age,
        c.gender,
        c.dbt_valid_from,
        c.dbt_valid_to,
        CASE WHEN c.dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(o.quantity * p.price_cents), 0) AS lifetime_value_cents
    FROM scd_customers AS c
    LEFT JOIN {{ ref('stg_orders') }} AS o ON c.customer_id = o.customer_id
    LEFT JOIN {{ ref('stg_products') }} AS p ON o.product_id = p.product_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)

SELECT * FROM customer_metrics
