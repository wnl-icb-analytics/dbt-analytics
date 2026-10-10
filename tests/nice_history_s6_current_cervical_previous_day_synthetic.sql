{{ config(tags=['monthly-full', 'nice-history']) }}

{% set profile = nice_ref('int_nice_cervical_screening_evidence', 'current') %}
{% set profile = profile | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_profile') %}

WITH synthetic_profile AS (
    SELECT
        -9641::NUMBER AS person_id,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        DATEADD(month, -1, CURRENT_DATE())::TIMESTAMP_NTZ AS latest_completed_date
),
actual AS (
    SELECT
        population.person_id,
        evidence.reporting_date,
        evidence.latest_completed_date
    FROM (SELECT -9641::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date) AS population
    INNER JOIN {{ profile }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 1
    OR COUNT_IF(reporting_date = CURRENT_DATE()
        AND latest_completed_date = DATEADD(month, -1, CURRENT_DATE())) <> 1
