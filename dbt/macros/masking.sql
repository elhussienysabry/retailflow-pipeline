-- masking.sql
-- PII obfuscation macros for Dynamic Data Masking.
--
-- Usage:
--   SELECT
--     {{ mask_string('first_name') }} AS first_name,
--     {{ mask_email('email') }} AS email
--   FROM {{ ref('stg_customers') }}
--
-- WHY:
--   These macros allow field-level Dynamic Data Masking at the dbt
--   transformation layer.  They are designed to be gated behind a
--   dbt variable so that unmasked values are available in dev/CI
--   while masked values are served to restricted consumption layers.
--
--   mask_string:  'Jane'     → 'J***e'
--   mask_email:   'jane@example.com' → 'j***e@example.com'
-- ============================================================================

{% macro mask_string(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN LENGTH({{ column_name }}) <= 2 THEN
            REPEAT('*', LENGTH({{ column_name }}))
        ELSE
            CONCAT(
                LEFT({{ column_name }}, 1),
                REPEAT('*', LENGTH({{ column_name }}) - 2),
                RIGHT({{ column_name }}, 1)
            )
    END
{% endmacro %}


{% macro mask_email(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN POSITION('@' IN {{ column_name }}) = 0 THEN {{ column_name }}
        WHEN LENGTH(SPLIT_PART({{ column_name }}, '@', 1)) <= 2 THEN
            CONCAT(
                REPEAT('*', LENGTH(SPLIT_PART({{ column_name }}, '@', 1))),
                '@',
                SPLIT_PART({{ column_name }}, '@', 2)
            )
        ELSE
            CONCAT(
                LEFT(SPLIT_PART({{ column_name }}, '@', 1), 1),
                REPEAT('*', LENGTH(SPLIT_PART({{ column_name }}, '@', 1)) - 2),
                RIGHT(SPLIT_PART({{ column_name }}, '@', 1), 1),
                '@',
                SPLIT_PART({{ column_name }}, '@', 2)
            )
    END
{% endmacro %}
