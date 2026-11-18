{{ config(materialized='table', cluster_by=['person_id']) }}

WITH register AS (
    {{ calculate_nice_hypothyroidism_register(reference='current') }}
)
SELECT person_id, reference_date AS reporting_date, age, practice_code, is_on_register, earliest_diagnosis_date, latest_diagnosis_date, latest_thyroid_diagnosis_date, latest_levothyroxine_order_date
FROM register
