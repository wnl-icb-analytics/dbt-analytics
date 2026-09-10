{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-person profile for the NICE chronic kidney disease indicators (IND130,
IND233, IND234, IND235, IND263, IND264, IND324). One row per person on the CKD
register (stage 3 to 5 by code). Combines the register diagnosis date, the
latest eGFR and urine ACR values, proteinuria records, frailty, diabetes type,
hypertension, the latest renin-angiotensin and SGLT2 inhibitor orders, ACE
inhibitor and ARB contraindications, and the eGFR and ACR tests around the
diagnosis that the new-diagnosis indicators need. No registration, living or
test-patient filter; consumers join dim_person_active_patients.
*/

WITH register AS (
    SELECT person_id, earliest_diagnosis_date::DATE AS ckd_diagnosis_date
    FROM {{ ref('fct_person_ckd_register') }}
    WHERE is_on_register
),

latest_egfr AS (
    SELECT person_id, clinical_effective_date::DATE AS latest_egfr_date, egfr_value AS latest_egfr_value
    FROM {{ ref('int_egfr_all') }}
    WHERE egfr_value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC) = 1
),

latest_acr AS (
    SELECT person_id, clinical_effective_date::DATE AS latest_acr_date, acr_value AS latest_acr_value
    FROM {{ ref('int_urine_acr_all') }}
    WHERE is_acr_ratio AND acr_value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC) = 1
),

proteinuria AS (
    SELECT
        person_id,
        BOOLOR_AGG(record_type IN ('PROTEINURIA', 'CKD_PROTEINURIA', 'PERSISTENT_PROTEINURIA')) AS has_proteinuria,
        BOOLOR_AGG(record_type = 'MICROALBUMINURIA') AS has_microalbuminuria
    FROM {{ ref('int_proteinuria_all') }}
    GROUP BY person_id
),

contraindication AS (
    -- Persisting at any time, or expiring in the preceding 12 months
    SELECT
        person_id,
        BOOLOR_AGG(drug_class = 'ACE_INHIBITOR' AND (is_persisting
            OR clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE()))) AS is_ace_inhibitor_contraindicated,
        BOOLOR_AGG(drug_class = 'ARB' AND (is_persisting
            OR clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE()))) AS is_arb_contraindicated
    FROM {{ ref('int_ras_contraindication_all') }}
    GROUP BY person_id
),

-- eGFR on two occasions at least 90 days apart, the second within 90 days before diagnosis
egfr_pair AS (
    SELECT DISTINCT r.person_id
    FROM register AS r
    INNER JOIN {{ ref('int_egfr_all') }} AS second
        ON r.person_id = second.person_id
        AND second.clinical_effective_date::DATE BETWEEN DATEADD(day, -90, r.ckd_diagnosis_date) AND r.ckd_diagnosis_date
    INNER JOIN {{ ref('int_egfr_all') }} AS first
        ON r.person_id = first.person_id
        AND first.clinical_effective_date::DATE <= DATEADD(day, -90, second.clinical_effective_date::DATE)
),

egfr_near_diagnosis AS (
    SELECT DISTINCT r.person_id
    FROM register AS r
    INNER JOIN {{ ref('int_egfr_all') }} AS egfr
        ON r.person_id = egfr.person_id
        AND egfr.clinical_effective_date::DATE BETWEEN DATEADD(day, -90, r.ckd_diagnosis_date) AND DATEADD(day, 90, r.ckd_diagnosis_date)
),

acr_near_diagnosis AS (
    SELECT DISTINCT r.person_id
    FROM register AS r
    INNER JOIN {{ ref('int_urine_acr_all') }} AS acr
        ON r.person_id = acr.person_id
        AND acr.is_acr_ratio
        AND acr.clinical_effective_date::DATE BETWEEN DATEADD(day, -90, r.ckd_diagnosis_date) AND DATEADD(day, 90, r.ckd_diagnosis_date)
)

SELECT
    r.person_id,
    r.ckd_diagnosis_date,
    latest_egfr.latest_egfr_date,
    latest_egfr.latest_egfr_value,
    latest_acr.latest_acr_date,
    latest_acr.latest_acr_value,
    COALESCE(proteinuria.has_proteinuria, FALSE) AS has_proteinuria,
    COALESCE(proteinuria.has_microalbuminuria, FALSE) AS has_microalbuminuria,
    frailty.latest_frailty_severity,
    diabetes.person_id IS NOT NULL AS has_diabetes,
    diabetes.diabetes_type,
    hypertension.person_id IS NOT NULL AS has_hypertension,
    ras.latest_order_date AS latest_ras_order_date,
    ras.latest_ras_class,
    COALESCE(contraindication.is_ace_inhibitor_contraindicated, FALSE) AS is_ace_inhibitor_contraindicated,
    COALESCE(contraindication.is_arb_contraindicated, FALSE) AS is_arb_contraindicated,
    sglt2.latest_order_date AS latest_sglt2_order_date,
    sglt2.latest_sglt2_drug,
    egfr_pair.person_id IS NOT NULL AS has_egfr_pair_before_diagnosis,
    egfr_near_diagnosis.person_id IS NOT NULL AS has_egfr_within_90_days_of_diagnosis,
    acr_near_diagnosis.person_id IS NOT NULL AS has_acr_within_90_days_of_diagnosis
FROM register AS r
LEFT JOIN latest_egfr ON r.person_id = latest_egfr.person_id
LEFT JOIN latest_acr ON r.person_id = latest_acr.person_id
LEFT JOIN proteinuria ON r.person_id = proteinuria.person_id
LEFT JOIN {{ ref('fct_person_frailty_register') }} AS frailty ON r.person_id = frailty.person_id
LEFT JOIN {{ ref('fct_person_diabetes_register') }} AS diabetes
    ON r.person_id = diabetes.person_id AND diabetes.is_on_register
LEFT JOIN {{ ref('fct_person_hypertension_register') }} AS hypertension
    ON r.person_id = hypertension.person_id AND hypertension.is_on_register
LEFT JOIN {{ ref('int_renin_angiotensin_therapy_latest') }} AS ras ON r.person_id = ras.person_id
LEFT JOIN contraindication ON r.person_id = contraindication.person_id
LEFT JOIN {{ ref('int_sglt2_therapy_latest') }} AS sglt2 ON r.person_id = sglt2.person_id
LEFT JOIN egfr_pair ON r.person_id = egfr_pair.person_id
LEFT JOIN egfr_near_diagnosis ON r.person_id = egfr_near_diagnosis.person_id
LEFT JOIN acr_near_diagnosis ON r.person_id = acr_near_diagnosis.person_id
