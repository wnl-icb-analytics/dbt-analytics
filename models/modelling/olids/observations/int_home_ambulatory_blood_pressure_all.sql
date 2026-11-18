{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Procedure records count even without a numeric BP component or complete pair.
SELECT
    id,
    person_id,
    clinical_effective_date,
    mapped_concept_code AS concept_code,
    mapped_concept_display AS concept_display
FROM ({{ get_observations("'HOMEAMBBP_COD'", source='PCD') }})
