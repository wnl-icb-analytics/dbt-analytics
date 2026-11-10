{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

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

WHERE is_eligible_egfr = TRUE
-- Ordering must match previous_egfr in int_ckd_lab_classification.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date::DATE DESC,
        date_recorded DESC NULLS LAST, id DESC
) = 1
