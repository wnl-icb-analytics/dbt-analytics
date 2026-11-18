{{ config(materialized='table', cluster_by=['month_end_date', 'person_id'], tags=['monthly-full']) }}

WITH register AS (
    {{ calculate_nice_bmi_register('obesity', reference='by_month', reference_dates=ltc_register_history_month_ends()) }}
)
SELECT person_id, reference_date AS month_end_date, age, practice_code, is_on_register, latest_bmi_date, bmi_value, bmi_source, requires_lower_bmi_thresholds, bmi_category, bmi_risk_sort_key
FROM register
