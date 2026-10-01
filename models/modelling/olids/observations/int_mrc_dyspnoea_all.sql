{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Medical Research Council dyspnoea scale records (MRC_COD), one row per
observation. mrc_grade is the grade 1 to 5 read from the code. Includes ALL
persons (active, inactive, deceased) following intermediate layer principles.
Observations dated after the build date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    REGEXP_SUBSTR(obs.mapped_concept_display, 'grade ([1-5])', 1, 1, 'i', 1)::INTEGER AS mrc_grade,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'MRC_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
