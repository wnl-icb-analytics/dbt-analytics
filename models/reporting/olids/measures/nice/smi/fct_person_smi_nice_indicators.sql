{{ config(materialized='table') }}

-- Common long-form interface for the NICE severe mental illness indicator views.
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
    latest_blood_pressure_date,
    latest_bmi_date,
    latest_alcohol_record_date,
    latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_physical_health_ind248') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_alcohol_ind82') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_bmi_ind83') }}

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
    latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_blood_pressure_ind84') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cholesterol_ind158') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_glucose_ind159') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_smoking_ind154') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_smoking_support_ind155') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_care_plan_ind143') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cvd_risk_assessment_ind150') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cervical_screening_ind85') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cervical_screening_ind213') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cervical_screening_ind214') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    latest_lithium_level_date,
    latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_lithium_monitoring_ind87') }}
