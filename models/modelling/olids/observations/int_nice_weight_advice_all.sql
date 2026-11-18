{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}
SELECT obs.id, obs.person_id, obs.clinical_effective_date, obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display
FROM ({{ get_observations("'WTMGINT_COD'", source='PCD') }}) AS obs
WHERE obs.clinical_effective_date IS NOT NULL
