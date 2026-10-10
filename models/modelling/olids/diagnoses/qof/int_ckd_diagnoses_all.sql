{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All chronic kidney disease (CKD) diagnosis observations from clinical records.
Uses QOF CKD cluster IDs:
- CKD_COD: CKD Stage 3-5 diagnoses (register inclusion)
- CKD1AND2_COD: CKD Stage 1-2 diagnoses (downstaging exclusion)
- CKDRES_COD: CKD resolved/remission codes

Clinical Purpose:
- QOF CKD register data collection
- Kidney function monitoring
- CKD staging and progression tracking
- Resolution status monitoring

QOF Context (v51):
CKD register includes persons aged 18+ with CKD Stage 3-5 diagnosis who have not:
- Been resolved (CKDRES_COD after CKD_COD)
- Been downstaged to Stage 1-2 (CKD1AND2_COD after CKD_COD)

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per CKD observation.
Use this model as input for fct_person_ckd_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- CKD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CKD_COD' THEN TRUE ELSE FALSE END AS is_stage_3_5_code,
    CASE WHEN obs.cluster_id = 'CKD1AND2_COD' THEN TRUE ELSE FALSE END AS is_stage_1_2_code,
    CASE WHEN obs.cluster_id = 'CKDRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,
    
    -- Backward compatibility alias (used by calculate_ckd_register macro and fct_person_condition_episodes)
    CASE WHEN obs.cluster_id = 'CKD_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,

    -- CKD observation type determination
    CASE
        WHEN obs.cluster_id = 'CKD_COD' THEN 'CKD Stage 3-5'
        WHEN obs.cluster_id = 'CKD1AND2_COD' THEN 'CKD Stage 1-2'
        WHEN obs.cluster_id = 'CKDRES_COD' THEN 'CKD Resolved'
        ELSE 'Unknown'
    END AS ckd_observation_type

FROM ({{ get_observations("'CKD_COD', 'CKD1AND2_COD', 'CKDRES_COD'", source='PCD', include_history=true) }}) obs

ORDER BY person_id, clinical_effective_date, id
