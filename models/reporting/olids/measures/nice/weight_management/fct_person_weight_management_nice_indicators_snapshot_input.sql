{{ config(materialized='view') }}
SELECT person_id, indicator_id, current_practice_code, latest_bmi_date, bmi_value, is_recorded_white, bmi_lower_threshold, bmi_upper_threshold, latest_weight_advice_date, latest_record_date, is_in_numerator, indicator_status,
    bmi_source, requires_lower_bmi_thresholds, bmi_category, timely_referral_date, timely_offer_date, timely_decline_date, latest_referral_date, latest_attendance_date, latest_end_date, has_previous_referral, is_currently_attending, has_hypertension, has_diabetes
FROM {{ ref('fct_person_weight_management_nice_indicators') }}
