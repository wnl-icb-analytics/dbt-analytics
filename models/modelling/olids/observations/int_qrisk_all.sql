{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All QRISK cardiovascular risk scores from observations.
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        TRY_CAST(obs.result_value AS FLOAT) AS qrisk_score,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value,

        -- Derive QRISK type from concept display
        CASE
            WHEN UPPER(obs.mapped_concept_display) LIKE '%QRISK3%' THEN 'QRISK3'
            WHEN UPPER(obs.mapped_concept_display) LIKE '%QRISK2%' THEN 'QRISK2'
            WHEN UPPER(obs.mapped_concept_display) LIKE '%QRISK%' THEN 'QRISK'
            ELSE 'Unknown'
        END AS qrisk_type

    FROM ({{ get_observations("'QRISKSCORE_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    qrisk_score,
    result_unit_display,
    qrisk_type,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

    -- Data quality validation (QRISK scores typically 0-100%)
    CASE
        WHEN qrisk_score BETWEEN 0 AND 100 THEN TRUE
        ELSE FALSE
    END AS is_valid_qrisk,

    -- Clinical risk categorisation (%) - CVD prevention guidelines
    CASE
        WHEN NOT is_valid_qrisk THEN NULL
        WHEN qrisk_score < 10 THEN 'Low Risk (<10%)'
        WHEN qrisk_score < 20 THEN 'Moderate Risk (10-20%)'
        WHEN qrisk_score >= 20 THEN 'High Risk (≥20%)'
        ELSE NULL
    END::VARCHAR(22) AS cvd_risk_category,

    -- High CVD risk indicator (≥10% 10-year risk)
    CASE
        WHEN qrisk_score >= 10 AND qrisk_score <= 100 THEN TRUE
        ELSE FALSE
    END AS is_high_cvd_risk,

    -- Very high CVD risk indicator (≥20% 10-year risk)
    CASE
        WHEN qrisk_score >= 20 AND qrisk_score <= 100 THEN TRUE
        ELSE FALSE
    END AS is_very_high_cvd_risk,

    -- Statin consideration indicator (≥10% typically warrants statin consideration)
    CASE
        WHEN qrisk_score >= 10 AND qrisk_score <= 100 THEN TRUE
        ELSE FALSE
    END AS warrants_statin_consideration

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
