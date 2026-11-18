{{ config(materialized='view') }}
SELECT person_id, indicator_id, current_practice_code, latest_bone_sparing_order_date, latest_record_date, is_in_numerator, indicator_status
FROM {{ ref('fct_person_bone_health_nice_indicators') }}
