{% macro medication_supply_columns(duration_days, order_date, cancellation_date) -%}
    {# Local supply estimate agreed in #1442. Duration is normalised upstream. #}
    case when {{ duration_days }} between 1 and 365
        then {{ duration_days }} else 28 end as effective_duration_days,
    coalesce({{ duration_days }} not between 1 and 365, true) as is_duration_imputed,
    least_ignore_nulls(
        dateadd(day, effective_duration_days, {{ order_date }}),
        dateadd(day, -1, to_date({{ cancellation_date }}))
    ) as supply_end_date
{%- endmacro %}
