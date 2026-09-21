{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All MASLD / NAFLD diagnosis observations from clinical records.
Uses the MASLD_DX_CODES cluster for terminology-managed coverage of modern MASLD
and legacy NAFLD/NASH diagnostic concepts.

Clinical Purpose:
- MASLD / NAFLD diagnosis tracking
- Liver health assessment
- Clinical register support for LTC LCS

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per diagnosis observation.
Use this model as input for fct_person_nafld_register.sql which applies person-level business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.patient_id,

    TRUE AS is_diagnosis_code,
    'MASLD / NAFLD Diagnosis' AS nafld_observation_type,
    (obs.result_value)::NUMBER(10, 2) AS numeric_value

FROM ({{ get_observations("'MASLD_DX_CODES'", source='ECL_CACHE', include_history=true) }}) obs

ORDER BY person_id, clinical_effective_date, id
