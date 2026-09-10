{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid waist circumference measurement per person.
Uses the comprehensive int_waist_circumference_all model and filters to most recent valid measurement.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    waist_circumference_value,
    concept_code,
    concept_display,
    source_cluster_id,
    waist_risk_category,
    is_high_waist_risk,
    is_very_high_waist_risk,
    original_result_value

FROM {{ ref('int_waist_circumference_all') }}

WHERE is_valid_waist_circumference = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
