{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Records that a person was asked about falls: a discussion about falls (FALLSDISC_COD) or a
falls risk assessment (FALRISKASS_COD), which cannot be done without asking. One row per
observation from the PCD clusters. Observations dated after the build date are excluded.
Includes ALL persons (active, inactive, deceased) following intermediate layer
principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'FALLSDISC_COD' THEN 'FALLS_DISCUSSION'
        WHEN 'FALRISKASS_COD' THEN 'FALLS_RISK_ASSESSMENT'
    END AS record_type,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'FALLSDISC_COD', 'FALRISKASS_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
