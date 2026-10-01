{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid GGT observation per person.
Excludes excluded units and negative values, returns the most recent per person. Extreme outliers
(above the biological upper limit in observation_value_bounds) are kept.
*/

SELECT
    id,
    person_id,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,
    original_result_value,
    original_result_unit_display,
    original_result_unit_code,
    expected_measurement_type,
    inferred_unit,
    inferred_value,
    value_was_converted,
    unit_was_changed,
    conversion_reason,
    confidence,
    ggt_category
FROM {{ ref('int_ggt_all') }}
WHERE inferred_value IS NOT NULL
  AND NOT is_negative
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
