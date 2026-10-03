{#-
    Combine the NICE weight management measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
{% macro nice_weight_management_union(reference='current') %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_bmi_date,
    bmi_value,
    is_recorded_white,
    bmi_lower_threshold,
    bmi_upper_threshold,
    latest_weight_advice_date,
    latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status
FROM {{ ref('fct_person_weight_management_advice_overweight_ind319' if reference == 'current' else 'fct_person_weight_management_advice_overweight_ind319_by_month') }}

UNION ALL

SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_bmi_date,
    NULL::FLOAT AS bmi_value,
    NULL::BOOLEAN AS is_recorded_white,
    NULL::FLOAT AS bmi_lower_threshold,
    NULL::FLOAT AS bmi_upper_threshold,
    NULL::DATE AS latest_weight_advice_date,
    latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status
FROM {{ ref('fct_person_bmi_recording_ind320' if reference == 'current' else 'fct_person_bmi_recording_ind320_by_month') }}
{% endmacro %}
