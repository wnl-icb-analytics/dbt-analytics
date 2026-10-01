{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid serum creatinine measurement per person.
Uses the comprehensive int_creatinine_all model and filters to most recent valid creatinine.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    creatinine_value,
    concept_code,
    concept_display,
    source_cluster_id,
    creatinine_category,
    is_elevated_creatinine,
    original_result_value

FROM {{ ref('int_creatinine_all') }}

WHERE is_valid_creatinine = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
