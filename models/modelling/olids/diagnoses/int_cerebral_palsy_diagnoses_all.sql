{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All cerebral palsy diagnosis observations from clinical records.
Uses cerebral palsy cluster ID:
- CEREBRALP_COD: Cerebral palsy diagnoses

Clinical Purpose:
- Cerebral palsy register data collection
- Neurological condition monitoring
- Care pathway tracking
- Support services planning

Clinical Context:
Cerebral palsy register includes persons with cerebral palsy diagnosis codes.
Cerebral palsy is a lifelong neurological condition with no resolution codes.
No age restrictions applied - condition is typically diagnosed in childhood but persists throughout life.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cerebral palsy observation.
Use this model as input for fct_person_cerebral_palsy_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Cerebral palsy-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Cerebral palsy observation type
    'Cerebral Palsy Diagnosis' AS cerebral_palsy_observation_type

FROM ({{ get_observations("'CEREBRALP_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

