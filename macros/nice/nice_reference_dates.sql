{% macro nice_reference_dates(reference='current') %}
{#-
    Supply today's date or the last 60 completed month-ends.
    Args: reference is current or by_month.
    Returns: reporting_date (DATE), one row per reference date.
-#}
{% if reference == 'current' %}
    SELECT CURRENT_DATE()::DATE AS reporting_date
{% elif reference == 'by_month' %}
    SELECT reference_date AS reporting_date
    FROM ({{ ltc_register_history_month_ends() }})
{% else %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{% endmacro %}
