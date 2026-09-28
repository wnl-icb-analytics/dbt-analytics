{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All venous thromboembolism (VTE) diagnosis observations from clinical
records.

Uses the QAdmissions 2023 VTE cluster:
- VENOUS_THROMBOEMBOLISM_COD_Q: Venous thromboembolism (combined deep vein
  thrombosis and pulmonary embolism codes, sourced from QResearch)

No resolution codes - diagnosis-only register. Prior VTE is treated as a
long-term risk marker, so the downstream feature uses "ever diagnosed".
No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate
layer principles. This is OBSERVATION-LEVEL data - one row per coded VTE
diagnosis. Use this model as input for int_qadmissions_features.sql,
where the wide feature row aggregates to the person grain.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code    AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id             AS source_cluster_id,
    TRUE                       AS is_diagnosis_code,
    'VTE Diagnosis'            AS vte_observation_type

FROM ({{ get_observations("'VENOUS_THROMBOEMBOLISM_COD_Q'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
