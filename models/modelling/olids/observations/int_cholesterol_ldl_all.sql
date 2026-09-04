{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH measurements AS (
    {{ get_lipid_observations('LDLCCHOL_COD', 'cholesterol_value') }}
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    clinical_effective_date_raw,
    date_recorded,
    cholesterol_value,
    result_unit_display,
    original_result_value,
    original_result_unit_code,
    original_result_unit_display,
    unit_status,
    concept_code,
    concept_display,
    source_cluster_id,
    sampling_context,
    COALESCE(cholesterol_value > 0 AND cholesterol_value < 'inf'::FLOAT, FALSE) AS is_valid_cholesterol,
    CASE
        WHEN NOT is_valid_cholesterol THEN 'Invalid'
        WHEN cholesterol_value < 3 THEN 'Below general reference limit'
        ELSE 'At or above general reference limit'
    END AS cholesterol_category,
    -- Retained for consumers; eligibility and reporting periods belong in measures.
    CASE
        WHEN NOT is_valid_cholesterol THEN 'Unknown'
        WHEN cholesterol_value <= 2 THEN 'Met'
        ELSE 'Not Met'
    END AS ldl_cvd_target_met
FROM measurements
