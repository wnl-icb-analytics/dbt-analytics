{% macro nice_ltc_summary(reference='current') %}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
SELECT person_id,
    {% if reference == 'current' %}CURRENT_DATE()::DATE{% else %}month_end_date{% endif %} AS reporting_date,
    condition_code, earliest_diagnosis_date, latest_diagnosis_date
FROM {{ ref('fct_person_ltc_summary' if reference == 'current' else 'fct_person_ltc_summary_by_month') }}
{% if reference == 'current' %}WHERE is_on_register{% endif %}
{% endmacro %}
