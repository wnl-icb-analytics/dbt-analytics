{{ config(materialized='view') }}

-- Keep evidence and outcomes; omit dates that advance every day.
SELECT
    person_id,
    indicator_id,
    current_practice_code,
    diagnosis_date,
    latest_acr_date,
    latest_home_ambulatory_bp_date,
    is_in_numerator,
    indicator_status,
    latest_ecg_date,
    has_12lead_ecg,
    latest_haematuria_test_date,
    has_blood_specific_test
FROM {{ ref('fct_person_hypertension_nice_indicators') }}
