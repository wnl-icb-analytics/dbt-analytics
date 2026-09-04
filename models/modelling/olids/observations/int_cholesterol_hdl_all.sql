{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH measurements AS (
    {{ get_lipid_observations('HDLCCHOL_COD', 'cholesterol_value') }}
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
        WHEN cholesterol_value <= 1 THEN '1.0 or below'
        WHEN cholesterol_value <= 1.2 THEN 'Above 1.0 to 1.2'
        ELSE 'Above 1.2'
    END AS cholesterol_category
FROM measurements
