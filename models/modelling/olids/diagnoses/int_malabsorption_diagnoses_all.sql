{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All malabsorption diagnosis observations from clinical records.

Uses the QAdmissions 2023 malabsorption cluster:
- MALABSORPTION_COD_Q: Malabsorption (covers coeliac disease, inflammatory
  bowel disease, and post-resection / surgical malabsorption codes in a
  single QResearch-curated set)

No resolution codes - diagnosis-only register.
No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate
layer principles. This is OBSERVATION-LEVEL data - one row per coded
malabsorption diagnosis. Use this model as input for
int_qadmissions_features.sql, where the wide feature row aggregates to
the person grain.
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
    'Malabsorption Diagnosis'  AS malabsorption_observation_type

FROM ({{ get_observations("'MALABSORPTION_COD_Q'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
