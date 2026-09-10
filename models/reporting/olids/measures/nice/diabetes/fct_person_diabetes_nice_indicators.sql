{{ config(materialized='table') }}

-- Common long-form interface for the NICE diabetes, NDH and gestational diabetes indicator views.
{% set indicator_models = [
    'fct_person_diabetes_hba1c_ind179',
    'fct_person_diabetes_hba1c_ind180',
    'fct_person_diabetes_bp_ind249',
    'fct_person_diabetes_acr_ind111',
    'fct_person_diabetes_retinal_screening_ind137',
    'fct_person_diabetes_foot_examination_ind160',
    'fct_person_ndh_glycaemic_test_ind172',
    'fct_person_gestational_diabetes_hba1c_ind173'
] %}

{% for indicator_model in indicator_models %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    current_practice_code,
    current_practice_name,
    latest_record_date,
    {% if indicator_model in ['fct_person_diabetes_hba1c_ind179', 'fct_person_diabetes_hba1c_ind180'] %}
    latest_hba1c_value,
    indicator_threshold AS hba1c_threshold,
    NULL AS latest_systolic_value,
    NULL AS latest_diastolic_value,
    latest_frailty_severity,
    {% elif indicator_model == 'fct_person_diabetes_bp_ind249' %}
    NULL AS latest_hba1c_value,
    NULL AS hba1c_threshold,
    latest_systolic_value,
    latest_diastolic_value,
    latest_frailty_severity,
    {% else %}
    NULL AS latest_hba1c_value,
    NULL AS hba1c_threshold,
    NULL AS latest_systolic_value,
    NULL AS latest_diastolic_value,
    NULL AS latest_frailty_severity,
    {% endif %}
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
