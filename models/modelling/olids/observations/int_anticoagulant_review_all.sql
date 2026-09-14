{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Anticoagulant and atrial fibrillation review records, one row per observation:
ORANTICOAGREVW_COD (anticoagulant medication review) and AFMON_COD (atrial
fibrillation annual review or monitoring), from PCD. review_type separates them.
Used for NICE IND169, review of anticoagulation in atrial fibrillation, where an
AF annual review is taken as evidence of the review NICE describes. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'ORANTICOAGREVW_COD' THEN 'ANTICOAGULANT_REVIEW'
        WHEN 'AFMON_COD' THEN 'AF_ANNUAL_REVIEW'
    END AS review_type,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'ORANTICOAGREVW_COD', 'AFMON_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
