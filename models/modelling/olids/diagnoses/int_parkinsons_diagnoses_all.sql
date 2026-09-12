{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All Parkinson's disease diagnosis observations from clinical records.
Uses Parkinson's cluster ID:
- PD_COD: Parkinson's disease diagnoses

Clinical Purpose:
- Parkinson's disease register data collection
- Neurological condition monitoring
- Care pathway tracking
- Medication management support

Clinical Context:
Parkinson's disease register includes persons with Parkinson's diagnosis codes.
Parkinson's is a progressive neurological condition with no resolution codes.
No age restrictions applied - condition can occur at any age though more common in older adults.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per Parkinson's observation.
Use this model as input for fct_person_parkinsons_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Parkinson's-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Parkinson's observation type
    'Parkinsons Disease Diagnosis' AS parkinsons_observation_type

FROM ({{ get_observations("'PD_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

