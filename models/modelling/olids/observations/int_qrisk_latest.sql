{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid QRISK cardiovascular risk score per person.
Uses the comprehensive int_qrisk_all model and filters to most recent valid QRISK.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    qrisk_score,
    qrisk_type,
    concept_code,
    concept_display,
    source_cluster_id,
    cvd_risk_category,
    is_high_cvd_risk,
    is_very_high_cvd_risk,
    warrants_statin_consideration,
    original_result_value

FROM {{ ref('int_qrisk_all') }}

WHERE is_valid_qrisk = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
