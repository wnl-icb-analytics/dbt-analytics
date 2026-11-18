{{ config(materialized='view') }}

SELECT person_id, indicator_id, current_practice_code, delivery_date, latest_record_date, is_in_numerator, indicator_status
FROM {{ ref('fct_person_maternity_nice_indicators') }}
