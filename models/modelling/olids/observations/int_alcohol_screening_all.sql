{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Alcohol screening records, one row per observation, from the PCD clusters FAST_COD,
AUDITC_COD and AUDIT_COD. screening_tool names the tool; score_value holds a
whole-number result within the tool's range; is_positive_screen applies the NICE
thresholds for hazardous drinking: FAST 3 or more, AUDIT-C 5 or more, and full
AUDIT 8 or more (the standard hazardous threshold, a local extension as NICE
names only FAST and AUDIT-C). AUDIT-PC is positive at 5 or more under government
guidance. An assessment code without a score counts as a
screen but cannot be positive. Includes ALL persons (active, inactive, deceased)
following intermediate layer principles.
*/

WITH base AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        {{ alcohol_screening_tool('obs.cluster_id', 'obs.mapped_concept_code') }} AS screening_tool,
        TRY_CAST(obs.result_value::VARCHAR AS FLOAT) AS numeric_result,
        obs.result_value AS original_result_value,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'FAST_COD', 'AUDITC_COD', 'AUDIT_COD'", source='PCD') }}) obs
    WHERE obs.clinical_effective_date <= CURRENT_DATE()
),

validated AS (
    SELECT *,
        {{ alcohol_score_is_valid('numeric_result', 'screening_tool') }} AS is_valid_score
    FROM base
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    screening_tool,
    CASE WHEN is_valid_score THEN numeric_result END AS score_value,
    original_result_value,
    COALESCE(
        is_valid_score AND (
            (screening_tool = 'FAST' AND numeric_result >= 3)
            OR (screening_tool = 'AUDIT-C' AND numeric_result >= 5)
            OR (screening_tool = 'AUDIT-PC' AND numeric_result >= 5)
            OR (screening_tool = 'AUDIT' AND numeric_result >= 8)),
        FALSE
    ) AS is_positive_screen,
    concept_code,
    concept_display,
    source_cluster_id
FROM validated
