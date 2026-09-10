{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Diabetes structured education programme records from the PCD clusters, one
row per observation and cluster: a referral (DSEP_COD) or an offer
(NDASEPOFF_COD). Includes ALL persons (active, inactive, deceased) following
intermediate layer principles. Observations dated after the build date are
excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'DSEP_COD' THEN 'REFERRED'
        WHEN 'NDASEPOFF_COD' THEN 'OFFERED'
    END AS record_type,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'DSEP_COD', 'NDASEPOFF_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
