{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All motor neurone disease diagnosis observations from clinical records.
Uses motor neurone disease cluster ID:
- MND_COD: Motor neurone disease diagnoses

Clinical Purpose:
- Motor neurone disease register data collection
- Neurological condition monitoring
- Care pathway tracking
- Palliative care planning support

Clinical Context:
Motor neurone disease register includes persons with MND diagnosis codes.
MND is a progressive neurological condition with no resolution codes.
No age restrictions applied - condition can occur at any age though more common in older adults.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per MND observation.
Use this model as input for fct_person_mnd_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- MND-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- MND observation type
    'Motor Neurone Disease Diagnosis' AS mnd_observation_type

FROM ({{ get_observations("'MND_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

