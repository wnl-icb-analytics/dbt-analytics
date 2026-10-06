{{ config(materialized='table', cluster_by=['month_end_date', 'person_id'], tags=['monthly-full']) }}

WITH register AS (
    {{ calculate_aki_register(reference='by_month', reference_dates=ltc_register_history_month_ends()) }}
)
SELECT person_id, reference_date AS month_end_date, age, practice_code, is_on_register, earliest_diagnosis_date, latest_diagnosis_date
FROM register
