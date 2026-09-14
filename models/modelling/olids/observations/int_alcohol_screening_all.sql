{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Alcohol screening records, one row per observation, from the PCD clusters FAST_COD,
AUDITC_COD and AUDIT_COD. screening_tool names the tool; score_value holds a
numeric result within the tool's range; is_positive_screen applies the NICE
thresholds for hazardous drinking: FAST 3 or more, AUDIT-C 5 or more, and full
AUDIT 8 or more (the standard hazardous threshold, a local extension as NICE
names only FAST and AUDIT-C). An assessment code without a score counts as a
screen but cannot be positive. Includes ALL persons (active, inactive, deceased)
following intermediate layer principles.
*/

WITH base AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        CASE obs.cluster_id
            WHEN 'FAST_COD' THEN 'FAST'
            WHEN 'AUDITC_COD' THEN 'AUDIT-C'
            WHEN 'AUDIT_COD' THEN 'AUDIT'
        END AS screening_tool,
        TRY_CAST(obs.result_value AS FLOAT) AS numeric_result,
        obs.result_value AS original_result_value,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'FAST_COD', 'AUDITC_COD', 'AUDIT_COD'", source='PCD') }}) obs
    WHERE obs.clinical_effective_date <= CURRENT_DATE()
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    screening_tool,
    CASE
        WHEN screening_tool = 'FAST' AND numeric_result BETWEEN 0 AND 16 THEN numeric_result
        WHEN screening_tool = 'AUDIT-C' AND numeric_result BETWEEN 0 AND 12 THEN numeric_result
        WHEN screening_tool = 'AUDIT' AND numeric_result BETWEEN 0 AND 40 THEN numeric_result
    END AS score_value,
    original_result_value,
    COALESCE(
        (screening_tool = 'FAST' AND numeric_result BETWEEN 3 AND 16)
        OR (screening_tool = 'AUDIT-C' AND numeric_result BETWEEN 5 AND 12)
        OR (screening_tool = 'AUDIT' AND numeric_result BETWEEN 8 AND 40),
        FALSE
    ) AS is_positive_screen,
    concept_code,
    concept_display,
    source_cluster_id
FROM base
