{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Serum or plasma fructosamine measurements from the PCD cluster SERFRUC_COD, one
row per observation. Fructosamine is measured instead of HbA1c when HbA1c is
unreliable, so NICE diabetes glycaemic indicators exclude people with a
fructosamine test in the period. Includes ALL persons (active, inactive,
deceased) following intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    TRY_CAST(obs.result_value AS FLOAT) AS fructosamine_value,
    obs.result_unit_display,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'SERFRUC_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
