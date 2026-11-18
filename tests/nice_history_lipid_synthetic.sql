{{ config(tags=['monthly-full', 'nice-history']) }}

{# Fusion 2.0.6 cannot parse this input's UUID type in a native unit test. #}
{% set calculation = nice_ind278('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation = calculation | replace(ref('int_cvd_secondary_prevention_population_by_month') | string, 'synthetic_cvd') %}
{% set calculation = calculation | replace(ref('int_cholesterol_ldl_all') | string, 'synthetic_ldl') %}
{% set calculation = calculation | replace(ref('int_cholesterol_non_hdl_all') | string, 'synthetic_non_hdl') %}

WITH synthetic_population AS (
    SELECT -9005::NUMBER AS person_id, column1::DATE AS reporting_date,
        30 AS age, 'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name,
        '1996-06-15'::DATE AS birth_date_approx, 'Female' AS gender
    FROM VALUES ('2026-08-31'), ('2026-09-30'), ('2027-09-30')
),
synthetic_cvd AS (
    SELECT person_id, reporting_date, TRUE AS has_chd, FALSE AS has_stroke_tia,
        FALSE AS has_pad, FALSE AS has_familial_hypercholesterolaemia,
        FALSE AS has_haemorrhagic_stroke
    FROM synthetic_population
),
synthetic_ldl AS (
    SELECT -9005::NUMBER AS person_id, column1::VARCHAR AS id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        1.5::FLOAT AS cholesterol_value, column3::BOOLEAN AS is_valid_cholesterol,
        'STANDARD' AS unit_status, 1.5::FLOAT AS recorded_value,
        'mmol/L' AS source_result_unit_display, 'mmol/L' AS mapped_result_unit_display,
        1::FLOAT AS conversion_factor, 'PLAUSIBLE' AS plausibility_status,
        FALSE AS is_lipid_review_required
    FROM VALUES ('SYN_L1', '2026-08-01 12:00:00', TRUE),
        ('SYN_L2', '2026-09-01 10:00:00', FALSE)
),
synthetic_non_hdl AS (
    SELECT -9005::NUMBER AS person_id, 'SYN_N1'::VARCHAR AS id,
        '2026-09-01 15:00:00'::TIMESTAMP_NTZ AS clinical_effective_date,
        1.5::FLOAT AS cholesterol_value, TRUE AS is_valid_cholesterol,
        'STANDARD' AS unit_status, 1.5::FLOAT AS recorded_value,
        'mmol/L' AS source_result_unit_display, 'mmol/L' AS mapped_result_unit_display,
        1::FLOAT AS conversion_factor, 'PLAUSIBLE' AS plausibility_status,
        FALSE AS is_lipid_review_required
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 3 OR COUNT_IF(
    reporting_date = '2026-08-31' AND latest_lipid_observation_id = 'SYN_L1'
    AND is_in_numerator AND indicator_status = 'ACHIEVED'
) <> 1 OR COUNT_IF(
    reporting_date = '2026-09-30' AND latest_lipid_observation_id = 'SYN_L2'
    AND lipid_type = 'LDL cholesterol' AND NOT is_in_numerator
    AND indicator_status = 'NOT_ASSESSABLE'
) <> 1 OR COUNT_IF(
    reporting_date = '2027-09-30' AND latest_lipid_observation_id IS NULL
    AND latest_lipid_date IS NULL AND latest_lipid_value IS NULL
    AND NOT is_in_numerator AND indicator_status = 'NOT_RECORDED_IN_PERIOD'
) <> 1
