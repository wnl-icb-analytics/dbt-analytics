-- NICE IND191 and IND195 require each review component in the measurement period.
SELECT person_id, indicator_id
FROM {{ ref('fct_person_copd_review_ind191') }}
WHERE is_in_numerator
    AND NOT COALESCE(
        latest_review_date >= measurement_period_start
        AND latest_copd_exacerbation_count_date >= measurement_period_start
        AND latest_mrc_dyspnoea_date >= measurement_period_start,
        FALSE
    )

UNION ALL

SELECT indicator.person_id, indicator.indicator_id
FROM {{ ref('fct_person_dementia_care_plan_ind142') }} AS indicator
INNER JOIN {{ ref('int_ltc_review_profile') }} AS profile
    ON indicator.person_id = profile.person_id
WHERE indicator.is_in_numerator
    AND NOT COALESCE(indicator.latest_review_date >= profile.earliest_dementia_diagnosis_date, FALSE)

UNION ALL

SELECT person_id, indicator_id
FROM {{ ref('fct_person_heart_failure_review_ind195') }}
WHERE is_in_numerator
    AND NOT COALESCE(
        latest_review_date >= measurement_period_start
        AND latest_nyha_date >= measurement_period_start
        AND latest_medication_review_date >= measurement_period_start,
        FALSE
    )
