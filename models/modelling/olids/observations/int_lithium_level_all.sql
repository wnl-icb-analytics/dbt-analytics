{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Serum lithium level results, one row per observation from the PINCER lithium
level codelist. lithium_level is the result in mmol/L where a numeric value is
recorded; is_in_therapeutic_range applies the 0.4 to 1.0 mmol/L range used by
NICE CG185 and the BNF. Includes ALL persons (active, inactive, deceased)
following intermediate layer principles. Observations dated after the build
date are excluded.
*/

WITH results AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        TRY_CAST(obs.result_value AS FLOAT) AS lithium_level,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'PINCER/LITH_LEV'", source='OPENCODELISTS') }}) obs
    WHERE obs.clinical_effective_date <= CURRENT_DATE()
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    lithium_level,
    result_unit_display,
    lithium_level IS NOT NULL AS is_result_recorded,
    COALESCE(lithium_level BETWEEN 0.4 AND 1.0, FALSE) AS is_in_therapeutic_range,
    concept_code,
    concept_display,
    source_cluster_id
FROM results
