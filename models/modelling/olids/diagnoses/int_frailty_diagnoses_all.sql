{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All coded frailty severity observations from the PCD frailty clusters:
- MILDFRAIL_COD: mild frailty, Clinical Frailty Scale levels 4-5
- MODFRAIL_COD: moderate frailty, Clinical Frailty Scale level 6
- SEVFRAIL_COD: severe frailty, Clinical Frailty Scale levels 7-8

Severity comes from the cluster that matched the code. Generic frailty codes with
no graded severity are not in these clusters and are not included. Calculated
frailty scores (eFI, eFI2) are separate models; this is coded diagnosis only.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per frailty observation.
Use this model as input for fct_person_frailty_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    TRUE AS is_diagnosis_code,
    CASE obs.cluster_id
        WHEN 'MILDFRAIL_COD' THEN 'Mild'
        WHEN 'MODFRAIL_COD' THEN 'Moderate'
        WHEN 'SEVFRAIL_COD' THEN 'Severe'
    END AS frailty_severity

FROM ({{ get_observations("'MILDFRAIL_COD', 'MODFRAIL_COD', 'SEVFRAIL_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
