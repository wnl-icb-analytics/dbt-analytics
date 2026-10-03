{#-
    Combine the NICE dementia measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
{% macro nice_dementia_union(reference='current') %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    diagnosis_date,
    fbc_date,
    calcium_date,
    glucose_date,
    renal_date,
    liver_date,
    thyroid_date,
    b12_date,
    folate_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_dementia_baseline_tests_ind80' if reference == 'current' else 'fct_person_dementia_baseline_tests_ind80_by_month') }}
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
    diagnosis_date,
    fbc_date,
    calcium_date,
    glucose_date,
    renal_date,
    liver_date,
    thyroid_date,
    b12_date,
    folate_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_dementia_baseline_tests_ind118' if reference == 'current' else 'fct_person_dementia_baseline_tests_ind118_by_month') }}
{% endmacro %}
