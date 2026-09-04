{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH measurements AS (
    {{ get_lipid_observations('TRIGLYC_COD', 'triglycerides_value', triglycerides=true) }}
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    clinical_effective_date_raw,
    date_recorded,
    triglycerides_value,
    result_unit_display,
    original_result_value,
    original_result_unit_code,
    original_result_unit_display,
    unit_status,
    concept_code,
    concept_display,
    source_cluster_id,
    sampling_context,
    COALESCE(triglycerides_value > 0 AND triglycerides_value < 'inf'::FLOAT, FALSE) AS is_valid_triglycerides,
    CASE
        WHEN NOT is_valid_triglycerides THEN 'Invalid'
        WHEN triglycerides_value < 1.7 THEN 'Below 1.7'
        WHEN triglycerides_value < 2 THEN '1.7 to below 2.0'
        ELSE '2.0 or above'
    END AS triglycerides_category
FROM measurements
