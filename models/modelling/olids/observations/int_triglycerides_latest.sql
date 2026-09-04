{{ config(materialized='table', cluster_by=['person_id']) }}

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
    is_valid_triglycerides,
    triglycerides_category
FROM {{ ref('int_triglycerides_all') }}
WHERE is_valid_triglycerides
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
