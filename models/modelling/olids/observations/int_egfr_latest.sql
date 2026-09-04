{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid eGFR measurement per person.
Uses the comprehensive int_egfr_all model and filters to most recent valid eGFR.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    egfr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    ckd_stage,
    is_ckd_indicator,
    original_result_value

FROM {{ ref('int_egfr_all') }}

WHERE is_valid_egfr = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
