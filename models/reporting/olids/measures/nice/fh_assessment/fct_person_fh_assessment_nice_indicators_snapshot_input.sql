{{ config(materialized='view') }}

SELECT
    person_id,
    indicator_id,
    current_practice_code,
    qualifying_reading_date,
    qualifying_cholesterol_value,
    age_at_earliest_qualifying_reading,
    first_fh_assessment_date,
    first_clinical_fh_diagnosis_date,
    first_fh_referral_date,
    first_genetic_fh_date,
    latest_secondary_hyperlipidaemia_date,
    first_secondary_history_date,
    has_qualifying_secondary_hyperlipidaemia,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_fh_assessment_nice_indicators') }}
