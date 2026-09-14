{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid urine ACR measurement per person.
Uses the comprehensive int_urine_acr_all model and filters to most recent valid ACR.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    acr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    acr_category,
    is_acr_elevated,
    is_microalbuminuria,
    is_macroalbuminuria,
    original_result_value

FROM {{ ref('int_urine_acr_all') }}

WHERE is_valid_acr = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
