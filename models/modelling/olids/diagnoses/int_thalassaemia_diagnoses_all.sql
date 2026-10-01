{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All thalassaemia diagnosis observations from clinical records.
Uses cluster THAL_COD (thalassaemia disease codes - e.g. alpha/beta thalassaemia,
thalassaemia major; excludes carrier/trait codes).

Thalassaemia is an inherited blood disorder where the body makes too little normal
haemoglobin, causing anaemia. Severe forms (such as thalassaemia major) need lifelong
regular blood transfusions plus treatment to remove the resulting excess iron. Most
common in people of Mediterranean, Middle Eastern, South Asian and South-East Asian
family backgrounds.

No resolution codes (lifelong condition). No age restriction.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
One row per observation. Feeds fct_person_thalassaemia_register.
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
    'Thalassaemia Diagnosis' AS thalassaemia_observation_type

FROM ({{ get_observations("'THAL_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
