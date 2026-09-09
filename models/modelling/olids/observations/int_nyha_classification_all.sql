{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
New York Heart Association classification records (NYHA_COD), one row per
observation. nyha_class is the class 1 to 4 read from a class-specific code;
assessment-only codes carry no class. Includes ALL persons (active, inactive,
deceased) following intermediate layer principles. Observations dated after the
build date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE REGEXP_SUBSTR(obs.mapped_concept_display, 'class (IV|III|II|I)\\b', 1, 1, 'i', 1)
        WHEN 'I' THEN 1
        WHEN 'II' THEN 2
        WHEN 'III' THEN 3
        WHEN 'IV' THEN 4
    END AS nyha_class,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'NYHA_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
