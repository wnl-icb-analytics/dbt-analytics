{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Obstructive sleep apnoea diagnosis records. One row per observation from the PCD cluster
OBSLPAPNOEA_COD. Observations dated after the build date are excluded.
Includes ALL persons (active, inactive, deceased) following intermediate layer
principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'OBSLPAPNOEA_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
