{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All autism spectrum disorder diagnosis observations from clinical records.
Uses autism cluster ID:
- AUTISM_COD: Autism spectrum disorder diagnoses

Clinical Purpose:
- Autism register data collection
- Neurodevelopmental condition monitoring
- Care pathway tracking
- Complex needs identification

Clinical Context:
Autism register includes persons with autism spectrum disorder diagnosis codes.
Autism is a lifelong neurodevelopmental condition with no resolution codes.
No age restrictions applied - condition is typically diagnosed in childhood but persists throughout life.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per autism observation.
Use this model as input for fct_person_autism_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Autism-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Autism observation type
    'Autism Spectrum Disorder Diagnosis' AS autism_observation_type

FROM ({{ get_observations("'AUTISM_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

