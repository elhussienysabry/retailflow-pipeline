{% snapshot scd_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='email',
        strategy='timestamp',
        updated_at='_execution_date',
        invalidate_hard_deletes=True,
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    city,
    signup_date,
    age,
    gender,
    _execution_date
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
