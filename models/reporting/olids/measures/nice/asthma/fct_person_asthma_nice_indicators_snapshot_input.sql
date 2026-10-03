{{ config(materialized='view') }}

SELECT
    person_id,
    indicator_id,
    current_practice_code,
    latest_record_date,
    saba_inhaler_count,
    oral_steroid_course_count,
    latest_asthma_admission_date,
    has_high_saba_use,
    has_repeated_oral_steroids,
    has_asthma_admission,
    latest_review_date,
    diagnosis_date,
    feno_date,
    reversibility_date,
    pefr_variability_date,
    bronchial_challenge_date,
    skin_prick_date,
    ige_date,
    eosinophil_or_fbc_date,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_asthma_nice_indicators') }}
