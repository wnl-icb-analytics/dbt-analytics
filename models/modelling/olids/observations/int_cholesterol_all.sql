{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH measurements AS (
    {{ get_lipid_observations('CHOL2_COD', 'cholesterol_value') }}
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
    plausibility_status = 'Within valid range' AS is_valid_cholesterol,
    CASE
        WHEN NOT is_valid_cholesterol THEN 'Invalid'
        WHEN cholesterol_value < 5 THEN 'Desirable'
        WHEN cholesterol_value < 6.2 THEN 'Borderline High'
        ELSE 'High'
    END AS cholesterol_category
FROM measurements
