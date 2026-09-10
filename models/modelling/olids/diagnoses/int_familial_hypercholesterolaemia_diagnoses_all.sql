{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.clinical_effective_date_raw,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- FH diagnoses are lifelong; resolution and age rules belong in consumers.
    CASE WHEN obs.cluster_id = 'FHYP_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code

FROM ({{ get_observations("'FHYP_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
