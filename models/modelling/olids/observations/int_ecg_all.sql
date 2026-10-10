{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT
    id,
    person_id,
    clinical_effective_date,
    cluster_id,
    mapped_concept_code AS concept_code,
    mapped_concept_display AS concept_display
FROM ({{ get_observations("'ECG_12LEAD_COD', 'ECG_GENERAL_COD'", source='ECL_CACHE') }})
