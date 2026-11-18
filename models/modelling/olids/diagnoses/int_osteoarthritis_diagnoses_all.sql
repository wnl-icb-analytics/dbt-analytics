{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All osteoarthritis diagnosis observations from clinical records.
Uses osteoarthritis cluster ID:
- OA_COD: Osteoarthritis diagnoses (<< 396275006, HISTORY-MAX supplement for inactive root)

Clinical Context:
Osteoarthritis is a degenerative joint condition with no resolution codes.
No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per osteoarthritis observation.
Use this model as input for fct_person_osteoarthritis_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Osteoarthritis-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Osteoarthritis observation type
    'Osteoarthritis Diagnosis' AS osteoarthritis_observation_type

FROM ({{ get_observations("'OA_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
