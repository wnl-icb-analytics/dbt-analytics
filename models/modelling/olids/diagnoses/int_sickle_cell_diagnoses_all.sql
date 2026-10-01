{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All sickle cell disease diagnosis observations from clinical records.
Uses cluster SICKLE_COD (sickle cell disease diagnoses only - no carrier/trait codes).

Sickle cell disease is an inherited blood disorder. Abnormal haemoglobin makes red
blood cells stiff and crescent ("sickle") shaped, causing episodes of severe pain
(crises), anaemia, frequent infections and higher stroke risk. It is lifelong and
most common in people of African and Caribbean family backgrounds.

No resolution codes (lifelong condition). No age restriction.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
One row per observation. Feeds fct_person_sickle_cell_register.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    TRUE AS is_diagnosis_code,
    'Sickle Cell Disease Diagnosis' AS sickle_cell_observation_type

FROM ({{ get_observations("'SICKLE_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
