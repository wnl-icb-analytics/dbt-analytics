{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All epilepsy diagnosis observations from clinical records.
Uses QOF epilepsy cluster IDs per v51 business rules:
- EPIL_COD: Epilepsy diagnoses
- EPILRES_COD: Epilepsy resolved codes

Clinical Purpose:
- QOF epilepsy register data collection
- Epilepsy care pathway monitoring
- Seizure management tracking
- Resolution status tracking

QOF Context:
Epilepsy register includes persons with epilepsy diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions typically ≥18 years applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per epilepsy observation.
Use this model as input for fct_person_epilepsy_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Epilepsy-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'EPIL_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'EPILRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- Epilepsy observation type determination
    CASE
        WHEN obs.cluster_id = 'EPIL_COD' THEN 'Epilepsy Diagnosis'
        WHEN obs.cluster_id = 'EPILRES_COD' THEN 'Epilepsy Resolved'
        ELSE 'Unknown'
    END AS epilepsy_observation_type

FROM ({{ get_observations("'EPIL_COD', 'EPILRES_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
