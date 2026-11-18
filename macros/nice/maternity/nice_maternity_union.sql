{% macro nice_maternity_union(reference='current') %}
{#-
    Combine NICE maternity measures with their delivery and screening detail.
    Args: reference is current or by_month.
    Returns: one person and indicator per reporting_date.
-#}
SELECT person_id, indicator_id, indicator_name, indicator_description, reporting_date, measurement_period_start,
    age, denominator_description, {{ nice_practice_columns(none, reference) }},
    delivery_date, latest_record_date, is_in_denominator, is_in_numerator, indicator_status
FROM {{ ref('fct_person_postnatal_mental_health_ind178' if reference == 'current' else 'fct_person_postnatal_mental_health_ind178_by_month') }}
{% endmacro %}
