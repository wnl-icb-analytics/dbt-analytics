{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH evidence AS (
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id,
    observation.mapped_concept_code as concept_code
FROM ({{ get_observations(observation_evidence_cluster_ids('dietary_review', 'ECL_CACHE'), source="ECL_CACHE") }}) AS observation
UNION ALL
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id,
    observation.mapped_concept_code as concept_code
FROM ({{ get_observations(observation_evidence_cluster_ids('dietary_review', 'PCD'), source="PCD") }}) AS observation
UNION ALL
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id,
    observation.mapped_concept_code as concept_code
FROM ({{ get_observations(observation_evidence_cluster_ids('dietary_review', 'OPENCODELISTS'), source="OPENCODELISTS") }}) AS observation
)
SELECT evidence.id, evidence.person_id, evidence.clinical_effective_date, evidence.source_cluster_id,
    evidence.concept_code, classification.evidence_type
FROM evidence
INNER JOIN {{ ref('snomed_evidence_type') }} AS classification
    ON evidence.concept_code = classification.snomed_code
WHERE classification.evidence_type = 'completed'
    AND NOT REGEXP_LIKE(LOWER(classification.concept_name), '.*(screening tool|appetite questionnaire).*')
QUALIFY ROW_NUMBER() OVER (PARTITION BY evidence.id ORDER BY evidence.source_cluster_id) = 1
