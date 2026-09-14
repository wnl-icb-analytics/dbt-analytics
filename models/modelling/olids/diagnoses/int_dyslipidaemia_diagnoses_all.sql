{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Dyslipidaemia and hyperlipidaemia diagnosis records from the local ECL cluster. One row per observation from the ECL_CACHE cluster
DYSLIPIDAEMIA_COD. Observations dated after the build date are excluded.
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
FROM ({{ get_observations("'DYSLIPIDAEMIA_COD'", source='ECL_CACHE') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
