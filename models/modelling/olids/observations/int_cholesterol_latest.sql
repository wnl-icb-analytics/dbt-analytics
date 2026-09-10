{{ config(materialized='table', cluster_by=['person_id']) }}

WITH results AS (
    SELECT
        *,
        MAX(clinical_effective_date) OVER (PARTITION BY person_id) AS most_recent_result_date
    FROM {{ ref('int_cholesterol_all') }}
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    clinical_effective_date_raw,
    date_recorded,
    cholesterol_value,
    result_unit_display,
    recorded_value,
    converted_value_mmol_l,
    source_result_unit_code,
    source_result_unit_display,
    mapped_result_unit_code,
    mapped_result_unit_display,
    conversion_unit_basis,
    conversion_factor,
    is_unit_metadata_conflict,
    plausibility_status,
    is_lipid_review_required,
    original_result_value,
    original_result_unit_code,
    original_result_unit_display,
    unit_status,
    concept_code,
    concept_display,
    source_cluster_id,
    sampling_context,
    is_valid_cholesterol,
    cholesterol_category,
    most_recent_result_date,
    -- The person's most recent result was invalid, so this row is an older valid result.
    most_recent_result_date > clinical_effective_date AS has_later_unassessable_result
FROM results
WHERE is_valid_cholesterol
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
