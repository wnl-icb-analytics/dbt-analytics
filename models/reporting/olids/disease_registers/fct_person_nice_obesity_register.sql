{{ config(materialized='table', cluster_by=['person_id']) }}

WITH register AS (
    {{ calculate_nice_bmi_register('obesity', reference='current') }}
)
SELECT person_id, reference_date AS reporting_date, age, practice_code, is_on_register, latest_bmi_date, bmi_value, is_recorded_white, bmi_threshold
FROM register
