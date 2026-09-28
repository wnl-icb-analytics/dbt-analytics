{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.clinical_effective_date_raw,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'HSTRK_COD'", source='PCD') }}) obs
