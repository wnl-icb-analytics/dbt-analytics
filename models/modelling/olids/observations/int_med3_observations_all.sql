{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All coded MED3 certification statuses from clinical records.

- MED3_CERTIFICATE: MED3 certification status (any coded MED3 certification status)

Includes ALL persons (active, inactive, deceased) following intermediate
layer principles. 

*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code    AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id             AS source_cluster_id

FROM ({{ get_observations("'MED3_CERTIFICATE'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
  AND obs.clinical_effective_date <= CURRENT_DATE()

ORDER BY person_id, clinical_effective_date, id