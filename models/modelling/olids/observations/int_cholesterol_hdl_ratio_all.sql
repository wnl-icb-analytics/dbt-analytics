{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH measurements AS (
    {{ get_lipid_observations('TCHOLHDL_COD', 'cholesterol_hdl_ratio', ratio=true) }}
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    clinical_effective_date_raw,
    date_recorded,
    cholesterol_hdl_ratio,
    result_unit_display,
    recorded_value,
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
    plausibility_status = 'Within valid range' AS is_valid_cholesterol_hdl_ratio
FROM measurements
