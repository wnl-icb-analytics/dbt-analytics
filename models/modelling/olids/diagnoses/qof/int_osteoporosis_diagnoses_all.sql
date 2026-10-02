{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All osteoporosis diagnosis observations from clinical records.
Uses QOF osteoporosis cluster ID:
- OSTEO_COD: Osteoporosis diagnoses

Clinical Purpose:
- QOF osteoporosis register data collection
- Bone health assessment
- Osteoporosis diagnosis tracking

Key QOF Requirements:
- Ages 50-74: OSTEO_COD, fracture on or after 1 April 2012 and DXA confirmation
- Ages 75+: OSTEO_COD and fracture on or after 1 April 2014; no DXA requirement
- Register models combine these diagnoses with fracture and DXA observations

Note: DXA scans and T-scores are handled in separate int_dxa_scans_all model.
The register requires DXA confirmation only for ages 50-74.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per osteoporosis observation.
Use this model as input for fct_person_osteoporosis_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Osteoporosis-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'OSTEO_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'OSTEO_COD' THEN 'Osteoporosis Diagnosis'
        ELSE 'Unknown'
    END AS osteoporosis_observation_type

FROM ({{ get_observations("'OSTEO_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
