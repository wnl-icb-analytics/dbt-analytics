{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    classification.evidence_type,
    COALESCE(classification.evidence_type IN ('completed', 'finding'), FALSE) AS is_completed_confirmation
FROM ({{ get_observations(observation_evidence_cluster_ids('heart_failure_confirmation'), source='PCD') }}) AS obs
LEFT JOIN {{ ref('snomed_evidence_type') }} AS classification
    ON obs.mapped_concept_code = classification.snomed_code
