{{ config(tags=['monthly-full', 'nice-history']) }}

-- Selected clinical evidence cannot follow the assessed month-end.
SELECT COUNT(*) AS invalid_rows
FROM {{ ref('int_nice_review_evidence_by_month') }}
WHERE GREATEST_IGNORE_NULLS(
    latest_asthma_review_date,
    latest_complete_asthma_review_date,
    latest_copd_review_date,
    latest_copd_exacerbation_count_date,
    latest_heart_failure_review_date,
    latest_rheumatoid_arthritis_review_date,
    latest_ld_health_check_date,
    latest_ld_health_action_plan_date,
    latest_dementia_care_plan_date,
    latest_structured_medication_review_date,
    latest_falls_discussion_date,
    latest_mrc_dyspnoea_date,
    latest_nyha_date,
    latest_thyroid_function_test_date,
    latest_smi_care_plan_date,
    latest_medication_review_date,
    first_cancer_care_review_after_diagnosis_date
) > reporting_date
HAVING COUNT(*) > 0
