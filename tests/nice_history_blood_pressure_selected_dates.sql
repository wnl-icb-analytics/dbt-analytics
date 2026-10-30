{{ config(tags=['monthly-full', 'nice-history']) }}

-- Selected BP evidence cannot come from after its assessment date.
SELECT COUNT(*) AS rows_total
FROM {{ ref('int_nice_blood_pressure_control_latest_by_context_by_month') }}
HAVING COUNT_IF(reading_date > reporting_date) > 0
