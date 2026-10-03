{{ config(materialized='view') }}

SELECT
    person_id,
    indicator_id,
    current_practice_code,
    latest_fev1_date,
    latest_fev1_percent_date,
    latest_fev1_percent,
    latest_very_severe_code_date,
    latest_spo2_value,
    diagnosis_date,
    mrc_date,
    latest_offer_date,
    first_invitation_date,
    last_invitation_date,
    latest_response_date,
    is_excluded_invitation_non_response,
    latest_record_date,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_copd_nice_indicators') }}
