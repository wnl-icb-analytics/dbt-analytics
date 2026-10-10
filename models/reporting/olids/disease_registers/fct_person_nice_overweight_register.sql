{{ config(materialized='table', cluster_by=['person_id']) }}

WITH register AS (
    {{ calculate_nice_bmi_register('overweight', reference='current') }}
)
SELECT person_id, reference_date AS reporting_date, age, practice_code, is_on_register, latest_bmi_date, bmi_value, bmi_source, requires_lower_bmi_thresholds, bmi_category, bmi_risk_sort_key
FROM register
