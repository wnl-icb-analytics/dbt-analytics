-- QOF v51 AST015 resolves the timing of the NICE review's separately coded elements.
SELECT indicator.person_id, indicator.indicator_id
FROM {{ ref('fct_person_asthma_review_ind273') }} AS indicator
WHERE indicator.is_in_numerator
    AND (
        NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_ltc_review_all') }} AS plan
            WHERE plan.person_id = indicator.person_id
                AND plan.review_type = 'ASTHMA_ACTION_PLAN'
                AND plan.clinical_effective_date::DATE = indicator.latest_record_date
        )
        OR NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_ltc_review_all') }} AS exacerbations
            WHERE exacerbations.person_id = indicator.person_id
                AND exacerbations.review_type = 'ASTHMA_EXACERBATION_COUNT'
                AND exacerbations.clinical_effective_date::DATE
                    > DATEADD(month, -1, indicator.latest_record_date)
                AND exacerbations.clinical_effective_date::DATE <= indicator.latest_record_date
        )
    )
