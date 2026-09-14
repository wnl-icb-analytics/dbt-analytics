{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Structured medication review records, the shared decision-making review NICE and the PCN service specification describe. One row per observation from the PCD cluster
STRUCTMEDRVW_COD. Observations dated after the build date are excluded.
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
FROM ({{ get_observations("'STRUCTMEDRVW_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
