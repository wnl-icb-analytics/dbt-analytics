{{ config(materialized='view') }}

SELECT person_id, indicator_id, current_practice_code, diagnosis_date, fbc_date, calcium_date, glucose_date, renal_date, liver_date, thyroid_date, b12_date, folate_date, latest_record_date, is_in_numerator, indicator_status
FROM {{ ref('fct_person_dementia_nice_indicators') }}
