{% macro calculate_nice_review_evidence(reference='current') %}
{#-
    Select condition-review dates and first cancer follow-up at each reference date.
    Args: reference is current or by_month.
    Returns: one active, non-test candidate person/date and nullable review dates.
             Depression follow-up stays in IND104; asthma components stay in its event model.
-#}
-- NICE review evidence, one candidate person and reference date.
WITH ltc_population AS (
    SELECT
        person_id,
        reporting_date,
        latest_cancer_diagnosis_date,
        has_copd,
        has_heart_failure,
        has_rheumatoid_arthritis,
        has_hypothyroidism,
        has_learning_disability,
        has_dementia,
        has_smi,
        has_cancer,
        latest_new_depression_diagnosis_date,
        latest_frailty_severity
    FROM {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
),

candidate_routes AS (
    SELECT
        person_id,
        reporting_date
    FROM ltc_population
    WHERE has_copd
        OR has_heart_failure
        OR has_rheumatoid_arthritis
        OR has_hypothyroidism
        OR has_learning_disability
        OR has_dementia
        OR has_smi
        OR has_cancer
        OR latest_new_depression_diagnosis_date IS NOT NULL
        OR latest_frailty_severity IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_asthma_diagnosis_population(reference) }})

    UNION ALL

    -- Additional categories can qualify without any register membership.
    SELECT
        person_id,
        reporting_date
    FROM {{ nice_ref('int_nice_multimorbidity_categories', reference) }} AS categories
    GROUP BY person_id, reporting_date
    HAVING COUNT(DISTINCT category_code) >= 4
),

candidates AS (
    SELECT
        population.person_id,
        population.reporting_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN (
        SELECT DISTINCT person_id, reporting_date
        FROM candidate_routes
    ) AS route
        ON population.person_id = route.person_id
        AND population.reporting_date = route.reporting_date
),

candidate_people AS (
    SELECT DISTINCT person_id
    FROM candidates
),

review_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        CASE
            WHEN review_type IN ('MEDICATION_REVIEW', 'HEART_FAILURE_MEDICATION_REVIEW')
                THEN 'CODED_MEDICATION_REVIEW'
            WHEN review_type IN ('DEMENTIA_CARE_PLAN', 'DEMENTIA_CARE_PLAN_REVIEW')
                THEN 'DEMENTIA_CARE_PLAN'
            ELSE review_type
        END AS evidence_type
    FROM {{ ref('int_ltc_review_all') }}
    WHERE review_type IN (
        'COPD_REVIEW',
        'COPD_EXACERBATION_COUNT',
        'HEART_FAILURE_REVIEW',
        'MEDICATION_REVIEW',
        'HEART_FAILURE_MEDICATION_REVIEW',
        'RHEUMATOID_ARTHRITIS_REVIEW',
        'LEARNING_DISABILITY_HEALTH_CHECK',
        'LEARNING_DISABILITY_HEALTH_ACTION_PLAN',
        'DEMENTIA_CARE_PLAN',
        'DEMENTIA_CARE_PLAN_REVIEW',
        'CANCER_CARE_REVIEW'
    )

    UNION ALL

    SELECT
        person_id,
        review_date AS event_date,
        'ASTHMA_REVIEW' AS evidence_type
    FROM {{ ref('int_nice_asthma_review_all') }}
    WHERE has_review

    UNION ALL

    SELECT
        person_id,
        review_date AS event_date,
        'COMPLETE_ASTHMA_REVIEW' AS evidence_type
    FROM {{ ref('int_nice_asthma_review_all') }}
    WHERE is_complete_review

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        'STRUCTURED_MEDICATION_REVIEW' AS evidence_type
    FROM {{ ref('int_structured_medication_review_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        'FALLS_DISCUSSION' AS evidence_type
    FROM {{ ref('int_falls_discussion_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        'MRC_DYSPNOEA' AS evidence_type
    FROM {{ ref('int_mrc_dyspnoea_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        'NYHA' AS evidence_type
    FROM {{ ref('int_nyha_classification_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        'THYROID_FUNCTION_TEST' AS evidence_type
    FROM {{ ref('int_thyroid_function_test_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        'SMI_CARE_PLAN' AS evidence_type
    FROM {{ ref('int_smi_care_plan_all') }}
),

daily_events AS (
    -- Date-only payloads make records on the same person/type/day interchangeable.
    SELECT DISTINCT
        event.person_id,
        event.event_date,
        event.evidence_type
    FROM review_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

evidence_types AS (
    SELECT column1::VARCHAR AS evidence_type
    FROM VALUES
        ('ASTHMA_REVIEW'),
        ('COMPLETE_ASTHMA_REVIEW'),
        ('COPD_REVIEW'),
        ('COPD_EXACERBATION_COUNT'),
        ('HEART_FAILURE_REVIEW'),
        ('CODED_MEDICATION_REVIEW'),
        ('RHEUMATOID_ARTHRITIS_REVIEW'),
        ('LEARNING_DISABILITY_HEALTH_CHECK'),
        ('LEARNING_DISABILITY_HEALTH_ACTION_PLAN'),
        ('DEMENTIA_CARE_PLAN'),
        ('STRUCTURED_MEDICATION_REVIEW'),
        ('FALLS_DISCUSSION'),
        ('MRC_DYSPNOEA'),
        ('NYHA'),
        ('THYROID_FUNCTION_TEST'),
        ('SMI_CARE_PLAN')
),

selected_dates AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        kind.evidence_type,
        event.event_date
    FROM candidates AS candidate
    CROSS JOIN evidence_types AS kind
    -- Each candidate selects at most one daily event per class, never every earlier event.
    ASOF JOIN daily_events AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
        AND kind.evidence_type = event.evidence_type
),

review_dates AS (
    SELECT
        person_id,
        reporting_date,
        MAX(IFF(evidence_type = 'ASTHMA_REVIEW', event_date, NULL)) AS latest_asthma_review_date,
        MAX(IFF(evidence_type = 'COMPLETE_ASTHMA_REVIEW', event_date, NULL)) AS latest_complete_asthma_review_date,
        MAX(IFF(evidence_type = 'COPD_REVIEW', event_date, NULL)) AS latest_copd_review_date,
        MAX(IFF(evidence_type = 'COPD_EXACERBATION_COUNT', event_date, NULL)) AS latest_copd_exacerbation_count_date,
        MAX(IFF(evidence_type = 'HEART_FAILURE_REVIEW', event_date, NULL)) AS latest_heart_failure_review_date,
        MAX(IFF(evidence_type = 'CODED_MEDICATION_REVIEW', event_date, NULL)) AS latest_coded_medication_review_date,
        MAX(IFF(evidence_type = 'RHEUMATOID_ARTHRITIS_REVIEW', event_date, NULL)) AS latest_rheumatoid_arthritis_review_date,
        MAX(IFF(evidence_type = 'LEARNING_DISABILITY_HEALTH_CHECK', event_date, NULL)) AS latest_ld_health_check_date,
        MAX(IFF(evidence_type = 'LEARNING_DISABILITY_HEALTH_ACTION_PLAN', event_date, NULL)) AS latest_ld_health_action_plan_date,
        MAX(IFF(evidence_type = 'DEMENTIA_CARE_PLAN', event_date, NULL)) AS latest_dementia_care_plan_date,
        MAX(IFF(evidence_type = 'STRUCTURED_MEDICATION_REVIEW', event_date, NULL)) AS latest_structured_medication_review_date,
        MAX(IFF(evidence_type = 'FALLS_DISCUSSION', event_date, NULL)) AS latest_falls_discussion_date,
        MAX(IFF(evidence_type = 'MRC_DYSPNOEA', event_date, NULL)) AS latest_mrc_dyspnoea_date,
        MAX(IFF(evidence_type = 'NYHA', event_date, NULL)) AS latest_nyha_date,
        MAX(IFF(evidence_type = 'THYROID_FUNCTION_TEST', event_date, NULL)) AS latest_thyroid_function_test_date,
        MAX(IFF(evidence_type = 'SMI_CARE_PLAN', event_date, NULL)) AS latest_smi_care_plan_date
    FROM selected_dates
    GROUP BY person_id, reporting_date
),

cancer_review AS (
    -- Keep the first later review even when it misses the indicator's 12-month deadline.
    SELECT
        anchor.person_id,
        anchor.reporting_date,
        MIN(event.event_date) AS first_cancer_care_review_after_diagnosis_date
    FROM ltc_population AS anchor
    INNER JOIN daily_events AS event
        ON anchor.person_id = event.person_id
        AND event.evidence_type = 'CANCER_CARE_REVIEW'
        AND event.event_date
            BETWEEN anchor.latest_cancer_diagnosis_date AND anchor.reporting_date
    GROUP BY anchor.person_id, anchor.reporting_date
)

SELECT
    candidate.person_id,
    candidate.reporting_date,
    reviews.latest_asthma_review_date,
    reviews.latest_complete_asthma_review_date,
    reviews.latest_copd_review_date,
    reviews.latest_copd_exacerbation_count_date,
    reviews.latest_heart_failure_review_date,
    reviews.latest_rheumatoid_arthritis_review_date,
    reviews.latest_ld_health_check_date,
    reviews.latest_ld_health_action_plan_date,
    reviews.latest_dementia_care_plan_date,
    reviews.latest_structured_medication_review_date,
    reviews.latest_falls_discussion_date,
    reviews.latest_mrc_dyspnoea_date,
    reviews.latest_nyha_date,
    reviews.latest_thyroid_function_test_date,
    reviews.latest_smi_care_plan_date,
    GREATEST_IGNORE_NULLS(reviews.latest_coded_medication_review_date,
        reviews.latest_structured_medication_review_date) AS latest_medication_review_date,
    cancer_review.first_cancer_care_review_after_diagnosis_date
FROM candidates AS candidate
LEFT JOIN review_dates AS reviews
    ON candidate.person_id = reviews.person_id
    AND candidate.reporting_date = reviews.reporting_date
LEFT JOIN cancer_review
    ON candidate.person_id = cancer_review.person_id
    AND candidate.reporting_date = cancer_review.reporting_date
{% endmacro %}
