{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All ADHD diagnosis and remission observations from clinical records.
Uses ADHD cluster IDs:
- ADHD_COD: ADHD diagnoses
- ADHDREM_COD: ADHD remission codes

ADHD can be managed/in remission but is a lifelong neurodevelopmental condition.
No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per ADHD observation.
Use this model as input for fct_person_adhd_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- ADHD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'ADHD_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'ADHDREM_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- ADHD observation type determination
    CASE
        WHEN obs.cluster_id = 'ADHD_COD' THEN 'ADHD Diagnosis'
        WHEN obs.cluster_id = 'ADHDREM_COD' THEN 'ADHD Remission'
        ELSE 'Unknown'
    END AS adhd_observation_type

FROM ({{ get_observations("'ADHD_COD', 'ADHDREM_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
