{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Long-term condition review, care plan, health check and respiratory exacerbation count records from the PCD
review clusters, one row per observation and cluster. review_type names the
record from its cluster: asthma reviews, action plans and exacerbation counts; COPD reviews and exacerbation counts; heart failure, rheumatoid arthritis and
depression reviews, medication and heart failure medication reviews, learning
disability health checks and health action plans, cancer care reviews, and
dementia care plans and care plan reviews. Includes ALL persons (active,
inactive, deceased) following intermediate layer principles. Observations dated
after the build date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'REV_COD' THEN 'ASTHMA_REVIEW'
        WHEN 'WRITPASTP_COD' THEN 'ASTHMA_ACTION_PLAN'
        WHEN 'ASTEXACB_COD' THEN 'ASTHMA_EXACERBATION_COUNT'
        WHEN 'COPDEXACB_COD' THEN 'COPD_EXACERBATION_COUNT'
        WHEN 'COPDRVW_COD' THEN 'COPD_REVIEW'
        WHEN 'HFRVW_COD' THEN 'HEART_FAILURE_REVIEW'
        WHEN 'HFMEDRVW_COD' THEN 'HEART_FAILURE_MEDICATION_REVIEW'
        WHEN 'MEDRVW_COD' THEN 'MEDICATION_REVIEW'
        WHEN 'RARTHRVW_COD' THEN 'RHEUMATOID_ARTHRITIS_REVIEW'
        WHEN 'HLTHCHK_COD' THEN 'LEARNING_DISABILITY_HEALTH_CHECK'
        WHEN 'HLTHAP_COD' THEN 'LEARNING_DISABILITY_HEALTH_ACTION_PLAN'
        WHEN 'MDRV_COD' THEN 'CANCER_CARE_REVIEW'
        WHEN 'DEMCP_COD' THEN 'DEMENTIA_CARE_PLAN'
        WHEN 'DEMCPRVW_COD' THEN 'DEMENTIA_CARE_PLAN_REVIEW'
        WHEN 'DEPRVW_COD' THEN 'DEPRESSION_REVIEW'
    END AS review_type,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'REV_COD', 'WRITPASTP_COD', 'ASTEXACB_COD', 'COPDEXACB_COD', 'COPDRVW_COD', 'HFRVW_COD', 'HFMEDRVW_COD', 'MEDRVW_COD', 'RARTHRVW_COD', 'HLTHCHK_COD', 'HLTHAP_COD', 'MDRV_COD', 'DEMCP_COD', 'DEMCPRVW_COD', 'DEPRVW_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
