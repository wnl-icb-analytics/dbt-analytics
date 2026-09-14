{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Alcohol usage status records (ALCUSAGE_COD): coded descriptions of a person's
drinking such as non-drinker, occasional or heavy drinker, one row per
observation. These count as a record of alcohol consumption alongside units
per week and screening tools. Includes ALL persons (active, inactive,
deceased) following intermediate layer principles. Observations dated after
the build date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'ALCUSAGE_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
