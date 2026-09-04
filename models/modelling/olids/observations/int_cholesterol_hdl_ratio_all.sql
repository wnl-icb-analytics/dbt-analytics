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
    original_result_value,
    original_result_unit_code,
    original_result_unit_display,
    unit_status,
    concept_code,
    concept_display,
    source_cluster_id,
    sampling_context,
    COALESCE(cholesterol_hdl_ratio > 0 AND cholesterol_hdl_ratio < 'inf'::FLOAT, FALSE) AS is_valid_cholesterol_hdl_ratio
FROM measurements
