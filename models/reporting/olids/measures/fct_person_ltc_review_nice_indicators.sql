{{ config(materialized='table') }}

-- Common long-form interface for the NICE long-term condition review indicator views.
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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_asthma_review_ind273') }}

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
    latest_review_date,
    latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_copd_review_ind191') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    latest_nyha_date,
    latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_heart_failure_review_ind195') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_rheumatoid_arthritis_review_ind110') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_hypothyroidism_tft_ind139') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_learning_disability_health_check_ind265') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    latest_health_action_plan_date,
    has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_learning_disability_health_check_ind266') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_cancer_care_review_ind223') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_dementia_care_plan_ind142') }}

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
    latest_review_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_depression_review_ind104') }}
