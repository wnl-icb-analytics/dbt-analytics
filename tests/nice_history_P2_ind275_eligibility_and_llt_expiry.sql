{{ config(tags=['monthly-full', 'nice-history']) }}
{% set calculation = nice_ind275('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_input_0') %}
{% set calculation = calculation | replace(ref('int_nice_therapy_evidence_by_month') | string, 'synthetic_input_1') %}
{% set calculation = calculation | replace(ref('int_cvd_risk_profile_by_month') | string, 'synthetic_input_2') %}

WITH synthetic_input_0 AS (
SELECT -9601::NUMBER AS person_id,
    '2026-04-30'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-10-31'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-11-30'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
),
synthetic_input_1 AS (
SELECT -9601::NUMBER AS person_id,
    '2026-04-30'::DATE AS reporting_date,
    '2026-04-30'::DATE AS latest_lipid_lowering_order_date,
    'STATIN'::VARCHAR AS latest_lipid_lowering_class,
    'Synthetic statin'::VARCHAR AS latest_lipid_lowering_product,
    TRUE AS is_latest_lipid_lowering_statin
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-10-31'::DATE AS reporting_date,
    '2026-04-30'::DATE AS latest_lipid_lowering_order_date,
    'STATIN'::VARCHAR AS latest_lipid_lowering_class,
    'Synthetic statin'::VARCHAR AS latest_lipid_lowering_product,
    TRUE AS is_latest_lipid_lowering_statin
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-11-30'::DATE AS reporting_date,
    '2026-04-30'::DATE AS latest_lipid_lowering_order_date,
    'STATIN'::VARCHAR AS latest_lipid_lowering_class,
    'Synthetic statin'::VARCHAR AS latest_lipid_lowering_product,
    TRUE AS is_latest_lipid_lowering_statin
),
synthetic_input_2 AS (
SELECT -9601::NUMBER AS person_id,
    '2026-04-30'::DATE AS reporting_date,
    10::NUMBER AS latest_cvd_risk_score,
    FALSE AS has_cvd_including_haemorrhagic_stroke,
    TRUE AS has_type2_diabetes,
    TRUE AS has_diabetes,
    'None'::VARCHAR AS latest_frailty_severity,
    10::NUMBER AS max_cvd_risk_score_12m,
    '2026-04-01'::DATE AS latest_low_cvd_risk_score_date_36m,
    '2026-04-01'::DATE AS latest_high_cvd_risk_score_date_36m,
    '2026-01-01'::DATE AS earliest_hypertension_date,
    '2026-01-01'::DATE AS earliest_type2_diabetes_date,
    FALSE AS has_ckd,
    FALSE AS has_familial_hypercholesterolaemia,
    FALSE AS has_type1_diabetes
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-10-31'::DATE AS reporting_date,
    10::NUMBER AS latest_cvd_risk_score,
    FALSE AS has_cvd_including_haemorrhagic_stroke,
    TRUE AS has_type2_diabetes,
    TRUE AS has_diabetes,
    'None'::VARCHAR AS latest_frailty_severity,
    10::NUMBER AS max_cvd_risk_score_12m,
    '2026-04-01'::DATE AS latest_low_cvd_risk_score_date_36m,
    '2026-04-02'::DATE AS latest_high_cvd_risk_score_date_36m,
    '2026-01-01'::DATE AS earliest_hypertension_date,
    '2026-01-01'::DATE AS earliest_type2_diabetes_date,
    FALSE AS has_ckd,
    FALSE AS has_familial_hypercholesterolaemia,
    FALSE AS has_type1_diabetes
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-11-30'::DATE AS reporting_date,
    10::NUMBER AS latest_cvd_risk_score,
    FALSE AS has_cvd_including_haemorrhagic_stroke,
    TRUE AS has_type2_diabetes,
    TRUE AS has_diabetes,
    'None'::VARCHAR AS latest_frailty_severity,
    10::NUMBER AS max_cvd_risk_score_12m,
    NULL::DATE AS latest_low_cvd_risk_score_date_36m,
    NULL::DATE AS latest_high_cvd_risk_score_date_36m,
    '2026-01-01'::DATE AS earliest_hypertension_date,
    '2026-01-01'::DATE AS earliest_type2_diabetes_date,
    FALSE AS has_ckd,
    FALSE AS has_familial_hypercholesterolaemia,
    FALSE AS has_type1_diabetes
),
expected AS (
SELECT -9601::NUMBER AS person_id,
    '2026-10-31'::DATE AS reporting_date,
    '2026-04-30'::DATE AS latest_lipid_lowering_order_date,
    TRUE AS is_in_numerator,
    'ACHIEVED'::VARCHAR AS indicator_status
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-11-30'::DATE AS reporting_date,
    '2026-04-30'::DATE AS latest_lipid_lowering_order_date,
    FALSE AS is_in_numerator,
    'NOT_TREATED_IN_PERIOD'::VARCHAR AS indicator_status
),
actual AS ({{ calculation }}),
actual_occurrences AS (
    SELECT person_id, reporting_date, latest_lipid_lowering_order_date,
        is_in_numerator, indicator_status, COUNT(*) AS occurrences
    FROM actual
    GROUP BY ALL
),
expected_occurrences AS (
    SELECT person_id, reporting_date, latest_lipid_lowering_order_date,
        is_in_numerator, indicator_status, COUNT(*) AS occurrences
    FROM expected
    GROUP BY ALL
),
differences AS (
    (SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
    UNION ALL
    (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences)
)
SELECT COUNT(*) AS failures
FROM differences
HAVING COUNT(*) > 0
