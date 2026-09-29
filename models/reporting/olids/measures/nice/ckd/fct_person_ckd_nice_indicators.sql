{{ config(materialized='table') }}

-- Common long-form interface for the NICE chronic kidney disease indicator views.
-- Detail columns a view does not emit are typed nulls for its branch.
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
    NULL::FLOAT AS latest_egfr_value,
    NULL::FLOAT AS latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_albumin_testing_ind144') }}

UNION ALL

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_ras_therapy_ind263') }}

UNION ALL

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_ras_therapy_ind130') }}

UNION ALL

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_sglt2_therapy_ind324') }}

UNION ALL

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
    NULL::FLOAT AS latest_egfr_value,
    latest_acr_value,
    latest_systolic_value,
    latest_diastolic_value,
    latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    latest_bp_date,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_bp_ind264') }}

UNION ALL

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
    NULL::FLOAT AS latest_egfr_value,
    latest_acr_value,
    latest_systolic_value,
    latest_diastolic_value,
    latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    latest_bp_date,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_bp_ind235') }}

UNION ALL

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_new_diagnosis_egfr_ind233') }}

UNION ALL

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_new_diagnosis_tests_ind234') }}
