{#-
    Combine the NICE asthma measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
{% macro nice_asthma_union(reference='current') %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    risk_period_start,
    risk_period_end,
    saba_inhaler_count,
    oral_steroid_course_count,
    latest_asthma_admission_date,
    has_high_saba_use,
    has_repeated_oral_steroids,
    has_asthma_admission,
    latest_review_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS feno_date,
    NULL::DATE AS reversibility_date,
    NULL::DATE AS pefr_variability_date,
    NULL::DATE AS bronchial_challenge_date,
    NULL::DATE AS skin_prick_date,
    NULL::DATE AS ige_date,
    NULL::DATE AS eosinophil_or_fbc_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS latest_mart_date
FROM {{ ref('fct_person_asthma_higher_risk_review_ind315' if reference == 'current' else 'fct_person_asthma_higher_risk_review_ind315_by_month') }}

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
    NULL::DATE AS risk_period_start,
    NULL::DATE AS risk_period_end,
    NULL::NUMBER AS saba_inhaler_count,
    NULL::NUMBER AS oral_steroid_course_count,
    NULL::DATE AS latest_asthma_admission_date,
    NULL::BOOLEAN AS has_high_saba_use,
    NULL::BOOLEAN AS has_repeated_oral_steroids,
    NULL::BOOLEAN AS has_asthma_admission,
    NULL::DATE AS latest_review_date,
    diagnosis_date,
    feno_date,
    reversibility_date,
    pefr_variability_date,
    bronchial_challenge_date,
    skin_prick_date,
    ige_date,
    eosinophil_or_fbc_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS latest_mart_date
FROM {{ ref('fct_person_asthma_objective_tests_ind272' if reference == 'current' else 'fct_person_asthma_objective_tests_ind272_by_month') }}

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
    NULL::DATE AS risk_period_start,
    NULL::DATE AS risk_period_end,
    NULL::NUMBER AS saba_inhaler_count,
    NULL::NUMBER AS oral_steroid_course_count,
    NULL::DATE AS latest_asthma_admission_date,
    NULL::BOOLEAN AS has_high_saba_use,
    NULL::BOOLEAN AS has_repeated_oral_steroids,
    NULL::BOOLEAN AS has_asthma_admission,
    NULL::DATE AS latest_review_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS feno_date,
    NULL::DATE AS reversibility_date,
    NULL::DATE AS pefr_variability_date,
    NULL::DATE AS bronchial_challenge_date,
    NULL::DATE AS skin_prick_date,
    NULL::DATE AS ige_date,
    NULL::DATE AS eosinophil_or_fbc_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS latest_mart_date
FROM {{ ref('fct_person_asthma_smoking_status_ind189' if reference == 'current' else 'fct_person_asthma_smoking_status_ind189_by_month') }}

UNION ALL

SELECT
    person_id, indicator_id, indicator_name, reporting_date, measurement_period_start, age, condition_name,
    {{ nice_practice_columns(none, reference) }},
    risk_period_start, risk_period_end, saba_inhaler_count, oral_steroid_course_count,
    latest_asthma_admission_date, has_high_saba_use, has_repeated_oral_steroids, has_asthma_admission,
    NULL::DATE AS latest_review_date, NULL::DATE AS diagnosis_date, NULL::DATE AS feno_date,
    NULL::DATE AS reversibility_date, NULL::DATE AS pefr_variability_date,
    NULL::DATE AS bronchial_challenge_date, NULL::DATE AS skin_prick_date,
    NULL::DATE AS ige_date, NULL::DATE AS eosinophil_or_fbc_date,
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    latest_therapy_order_date, latest_mart_date
FROM {{ ref('fct_person_asthma_mart_ind316' if reference == 'current' else 'fct_person_asthma_mart_ind316_by_month') }}
{% endmacro %}
