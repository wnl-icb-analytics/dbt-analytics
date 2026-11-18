{{ config(tags=['monthly-full', 'nice-history']) }}
{% set calculation = nice_ind231('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_input_0') %}
{% set calculation = calculation | replace(ref('int_nice_therapy_evidence_by_month') | string, 'synthetic_input_1') %}
{% set calculation = calculation | replace(ref('fct_person_ckd_register_by_month') | string, 'synthetic_input_2') %}
{% set calculation = calculation | replace(ref('int_haemorrhagic_stroke_diagnoses_all') | string, 'synthetic_input_3') %}

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
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-12-31'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-12-01'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
UNION ALL
SELECT -9604::NUMBER AS person_id,
    '2026-04-30'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
UNION ALL
SELECT -9604::NUMBER AS person_id,
    '2026-10-31'::DATE AS reporting_date,
    50::NUMBER AS age,
    '1976-01-01'::DATE AS birth_date_approx,
    'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code,
    'Synthetic practice'::VARCHAR AS practice_name
UNION ALL
SELECT -9604::NUMBER AS person_id,
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
    '2026-04-30'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-10-31'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-11-30'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-12-31'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9601::NUMBER AS person_id,
    '2026-12-01'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9604::NUMBER AS person_id,
    '2026-04-30'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9604::NUMBER AS person_id,
    '2026-10-31'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
UNION ALL
SELECT -9604::NUMBER AS person_id,
    '2026-11-30'::DATE AS month_end_date,
    '2020-01-01'::DATE AS earliest_diagnosis_date,
    '2020-01-01'::DATE AS latest_diagnosis_date,
    NULL::DATE AS latest_resolved_date,
    NULL::DATE AS latest_stage_1_2_date,
    'Type 1'::VARCHAR AS diabetes_type,
    '2020-01-01'::DATE AS earliest_type1_date,
    '2020-01-01'::DATE AS latest_type1_date,
    NULL::DATE AS earliest_type2_date,
    NULL::DATE AS latest_type2_date
),
synthetic_input_3 AS (
SELECT -9601::NUMBER AS person_id,
    '2026-12-01'::DATE AS clinical_effective_date_raw
UNION ALL
SELECT -9604::NUMBER AS person_id,
    NULL::DATE AS clinical_effective_date_raw
),
expected AS (
SELECT -9601::NUMBER AS person_id,
    '2026-04-30'::DATE AS reporting_date,
    '2026-04-30'::DATE AS latest_lipid_lowering_order_date,
    TRUE AS is_in_numerator,
    'ACHIEVED'::VARCHAR AS indicator_status
UNION ALL
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
