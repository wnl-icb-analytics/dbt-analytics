{{ config(materialized='view') }}
SELECT person_id, indicator_id, current_practice_code, latest_bmi_date, bmi_value, is_recorded_white, bmi_lower_threshold, bmi_upper_threshold, latest_weight_advice_date, latest_record_date, is_in_numerator, indicator_status
FROM {{ ref('fct_person_weight_management_nice_indicators') }}
