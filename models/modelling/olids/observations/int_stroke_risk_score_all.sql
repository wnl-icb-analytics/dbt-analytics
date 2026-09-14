{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Atrial fibrillation stroke risk scores from the PCD clusters CHADVASC_COD
(CHA2DS2-VASc) and CHAD_COD (CHADS2), one row per observation. Score values
are integers from 0 to 9 (CHA2DS2-VASc) or 0 to 6 (CHADS2); other values are
kept in original_result_value and score_value is null. Includes ALL persons
(active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'CHADVASC_COD' THEN 'CHA2DS2-VASc'
        WHEN 'CHAD_COD' THEN 'CHADS2'
    END AS score_type,
    CASE
        WHEN obs.cluster_id = 'CHADVASC_COD'
            AND TRY_CAST(obs.result_value AS FLOAT) BETWEEN 0 AND 9
            AND TRY_CAST(obs.result_value AS FLOAT) = FLOOR(TRY_CAST(obs.result_value AS FLOAT))
            THEN FLOOR(TRY_CAST(obs.result_value AS FLOAT))::INTEGER
        WHEN obs.cluster_id = 'CHAD_COD'
            AND TRY_CAST(obs.result_value AS FLOAT) BETWEEN 0 AND 6
            AND TRY_CAST(obs.result_value AS FLOAT) = FLOOR(TRY_CAST(obs.result_value AS FLOAT))
            THEN FLOOR(TRY_CAST(obs.result_value AS FLOAT))::INTEGER
    END AS score_value,
    obs.result_value AS original_result_value,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'CHADVASC_COD', 'CHAD_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
