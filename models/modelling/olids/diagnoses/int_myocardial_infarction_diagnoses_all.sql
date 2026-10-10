{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display
FROM ({{ get_observations("'MI_COD'", source='PCD', include_history=true) }}) AS obs
