{{ config(tags=['monthly-full', 'nice-history']) }}

WITH failures AS (
    SELECT COUNT(*) AS failure_count
    FROM {{ ref('fct_person_cholesterol_control_ind278_by_month') }}
    WHERE latest_lipid_date::DATE > reporting_date
    UNION ALL
    SELECT COUNT(*)
    FROM {{ ref('fct_person_asthma_review_ind273_by_month') }}
    WHERE latest_review_date > reporting_date OR latest_record_date > reporting_date
    UNION ALL
    SELECT COUNT(*)
    FROM {{ ref('fct_person_depression_review_ind104_by_month') }}
    WHERE diagnosis_date > reporting_date OR latest_review_date > reporting_date
        OR latest_review_date < DATEADD(day, 10, diagnosis_date)
        OR latest_review_date > DATEADD(day, 35, diagnosis_date)
)
SELECT SUM(failure_count) AS failure_count FROM failures HAVING SUM(failure_count) > 0
