{{ config(tags=['monthly-full', 'nice-history']) }}

WITH dates AS (
    SELECT DISTINCT reporting_date FROM {{ ref('int_nice_reference_population_by_month') }}
)
SELECT COUNT(*) AS date_count
FROM dates
HAVING COUNT(*) <> 60
    OR COUNT_IF(reporting_date IS NULL) > 0
    OR MIN(reporting_date) <> LAST_DAY(DATEADD(month, -60, CURRENT_DATE()))
    OR MAX(reporting_date) <> LAST_DAY(DATEADD(month, -1, CURRENT_DATE()))
    OR COUNT_IF(reporting_date <> LAST_DAY(reporting_date)) > 0
