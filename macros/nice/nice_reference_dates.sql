{% macro nice_reference_dates(reference='current') %}
{% if reference == 'current' %}
    SELECT CURRENT_DATE()::DATE AS reporting_date
{% elif reference == 'by_month' %}
    SELECT reference_date AS reporting_date FROM ({{ ltc_register_history_month_ends() }})
{% else %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{% endmacro %}
