{{ config(materialized='table', cluster_by=['month_end_date', 'person_id'], tags=['monthly-full']) }}

WITH register AS (
    {{ calculate_nice_bmi_register('overweight', reference='by_month', reference_dates=ltc_register_history_month_ends()) }}
)
SELECT person_id, reference_date AS month_end_date, age, practice_code, is_on_register, latest_bmi_date, bmi_value, is_recorded_white, bmi_threshold
FROM register
