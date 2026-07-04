-- cents_to_dollars.sql
-- Macro: Convert an amount in cents to dollars.
--
-- Usage:
--   SELECT {{ cents_to_dollars('price_cents') }} AS price_dollars FROM ...
--
-- WHY: This macro exists because raw data stores prices in cents (integer)
-- to avoid floating-point precision issues. Business users think in dollars.
-- Centralizing the conversion in a macro ensures consistency across models.

{% macro cents_to_dollars(column_name, decimal_places=2) %}
    ROUND(CAST({{ column_name }} AS NUMERIC) / 100.0, {{ decimal_places }})
{% endmacro %}
