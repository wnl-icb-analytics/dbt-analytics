{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All multiple sclerosis diagnosis observations from clinical records.
Uses multiple sclerosis cluster ID:
- MS_COD: Multiple sclerosis diagnoses

Clinical Purpose:
- Multiple sclerosis register data collection
- Neurological condition monitoring
- Care pathway tracking
- Disease-modifying therapy monitoring

Clinical Context:
Multiple sclerosis register includes persons with MS diagnosis codes.
MS is a chronic neurological condition with no resolution codes.
No age restrictions applied - condition typically diagnosed in young to middle-aged adults.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per MS observation.
Use this model as input for fct_person_ms_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- MS-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- MS observation type
    'Multiple Sclerosis Diagnosis' AS ms_observation_type

FROM ({{ get_observations("'MS_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

