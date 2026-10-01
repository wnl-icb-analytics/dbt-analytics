{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest eosinophil count per person.
Filters to observations with a non-null inferred value that are non-negative and not
extreme outliers (> 100 x10*9/L), then returns the most recent per person.
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
    eosinophil_category
FROM {{ ref('int_eosinophil_count') }}
WHERE inferred_value IS NOT NULL AND NOT is_negative AND NOT is_extreme_outlier
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
