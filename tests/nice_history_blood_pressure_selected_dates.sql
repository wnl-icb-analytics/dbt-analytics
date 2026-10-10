{{ config(tags=['monthly-full', 'nice-history']) }}

-- Selected BP evidence cannot come from after its assessment date.
SELECT COUNT(*) AS rows_total
FROM {{ ref('int_nice_blood_pressure_latest_by_month') }}
HAVING COUNT_IF(latest_bp_date > reporting_date) > 0
