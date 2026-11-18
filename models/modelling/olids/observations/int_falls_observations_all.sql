{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All coded falls events from clinical records.

Uses the QAdmissions 2023 falls cluster:
- FALLS_COD_Q: Falls (any coded fall event - includes finding and event codes)

Includes ALL persons (active, inactive, deceased) following intermediate
layer principles. Use this model as input for qadmissions_input_features.sql,
where the wide feature row aggregates to the person grain.

*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code    AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id             AS source_cluster_id

FROM ({{ get_observations("'FALLS_COD_Q'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
  AND obs.clinical_effective_date <= CURRENT_DATE()

ORDER BY person_id, clinical_effective_date, id
