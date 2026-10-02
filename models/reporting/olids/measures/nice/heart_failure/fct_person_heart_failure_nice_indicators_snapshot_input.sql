{{ config(materialized='view') }}

SELECT
    person_id,
    indicator_id,
    current_practice_code,
    latest_ras_order_date,
    is_ras_in_period,
    latest_beta_blocker_order_date,
    is_beta_blocker_in_period,
    latest_hf_licensed_beta_blocker_order_date,
    is_hf_licensed_beta_blocker_in_period,
    latest_mra_order_date,
    is_mra_in_period,
    latest_sglt2_order_date,
    is_sglt2_in_period,
    diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_heart_failure_nice_indicators') }}
