{% macro blood_pressure_control_latest_contexts() %}
{# Latest eligible control day, one person/context, without a register restriction. #}
SELECT *
FROM {{ ref('int_blood_pressure_control_daily_by_context') }}
QUALIFY reading_date = MAX(reading_date) OVER (PARTITION BY person_id)
{% endmacro %}
