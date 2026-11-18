{{ config(materialized='table', cluster_by=['person_id', 'review_date']) }}

WITH daily_records AS (
    SELECT person_id, clinical_effective_date::DATE AS review_date,
        BOOLOR_AGG(review_type = 'ASTHMA_REVIEW') AS has_review,
        BOOLOR_AGG(review_type = 'ASTHMA_ACTION_PLAN') AS has_action_plan,
        BOOLOR_AGG(review_type = 'ASTHMA_EXACERBATION_COUNT') AS has_exacerbation_count
    FROM {{ ref('int_ltc_review_all') }}
    WHERE review_type IN ('ASTHMA_REVIEW', 'ASTHMA_ACTION_PLAN', 'ASTHMA_EXACERBATION_COUNT')
    GROUP BY person_id, clinical_effective_date::DATE
)
SELECT review.person_id, review.review_date, review.has_review, review.has_action_plan,
    review.has_exacerbation_count,
    review.has_review AND review.has_action_plan
        AND COUNT(exacerbations.person_id) > 0 AS is_complete_review
FROM daily_records review
LEFT JOIN daily_records exacerbations ON review.person_id = exacerbations.person_id
    AND exacerbations.has_exacerbation_count
    -- QOF v51 AST015 excludes the same date one month earlier.
    AND exacerbations.review_date > DATEADD(month, -1, review.review_date)
    AND exacerbations.review_date <= review.review_date
GROUP BY review.person_id, review.review_date, review.has_review, review.has_action_plan,
    review.has_exacerbation_count
