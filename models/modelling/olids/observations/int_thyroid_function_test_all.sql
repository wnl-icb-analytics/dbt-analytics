{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Thyroid function test records (TFT_COD), one row per observation, keeping the
result value and unit where recorded. Includes ALL persons (active, inactive,
deceased) following intermediate layer principles. Observations dated after the
build date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.result_value,
    obs.result_unit_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'TFT_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
