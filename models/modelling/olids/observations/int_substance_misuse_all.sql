{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Classified ILLSUB events for all people, including inactive and deceased people.
SELECT
    observation.id,
    observation.person_id,
    observation.clinical_effective_date,
    observation.date_recorded,
    observation.mapped_concept_code AS concept_code,
    observation.mapped_concept_display AS concept_display,
    -- Unclassified diagnosis-led codes retain the reviewed qualifying default.
    COALESCE(classification.status, 'QUALIFYING') AS status,
    classification.category
FROM ({{ get_observations("'ILLSUB_COD'") }}) AS observation
LEFT JOIN {{ ref('substance_misuse_illsub_status') }} AS classification
    ON observation.mapped_concept_code = classification.snomed_code
WHERE observation.clinical_effective_date IS NOT NULL
    AND observation.clinical_effective_date <= CURRENT_DATE()
