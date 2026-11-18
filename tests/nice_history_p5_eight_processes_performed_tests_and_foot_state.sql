{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind120('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('DM', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_egfr_test_all') | string, 'synthetic_tests') %}
{% set calculation = calculation | replace(ref('int_urine_acr_all') | string, 'synthetic_tests') %}
{% set calculation = calculation | replace(ref('int_cholesterol_all') | string, 'synthetic_tests') %}
{% set calculation = calculation | replace(ref('int_foot_examination_all') | string, 'synthetic_foot') %}
{% set calculation = calculation | replace(ref('int_nice_hba1c_evidence_by_month') | string, 'synthetic_hba') %}
{% set calculation = calculation | replace(ref('int_nice_physical_health_evidence_by_month') | string, 'synthetic_physical') %}
{% set calculation = calculation | replace(ref('int_nice_smoking_evidence_by_month') | string, 'synthetic_smoking') %}

WITH
synthetic_population AS (
    SELECT -9516::NUMBER AS person_id, column1::DATE AS reporting_date, 17 AS age,
        'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES ('2024-09-30'), ('2024-10-31'), ('2025-10-31')
),
synthetic_register AS (
    SELECT person_id, reporting_date
    FROM synthetic_population
),
synthetic_tests AS (
    SELECT -9516::NUMBER AS person_id, '2024-09-01'::DATE AS clinical_effective_date,
        TRUE AS is_acr_ratio, TRUE AS is_valid_cholesterol
),
synthetic_foot AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::BOOLEAN AS left_foot_checked,
        column4::BOOLEAN AS right_foot_checked,
        (left_foot_checked AND right_foot_checked) AS both_feet_checked,
        column5::BOOLEAN AS has_risk_classification,
        'Low Risk' AS diabetes_foot_risk_category,
        NULL::DATE AS first_left_foot_absent_date,
        column6::DATE AS first_right_foot_absent_date,
        NULL::DATE AS first_left_foot_amputated_date,
        column7::DATE AS first_right_foot_amputated_date
    FROM VALUES (-9516, '2024-09-01', TRUE, FALSE, FALSE, NULL, '2024-10-01'),
        (-9516, '2024-10-15', FALSE, FALSE, FALSE, NULL, '2024-10-01')
),
synthetic_hba AS (
    SELECT person_id, reporting_date, '2024-09-01'::DATE AS latest_hba1c_date
    FROM synthetic_population
),
synthetic_physical AS (
    SELECT person_id, reporting_date,
        '2024-09-01'::DATE AS latest_blood_pressure_date,
        '2024-09-01'::DATE AS latest_bmi_date
    FROM synthetic_population
),
synthetic_smoking AS (
    SELECT person_id, reporting_date, '2024-09-01'::DATE AS latest_smoking_status_date
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 3
    OR COUNT_IF(reporting_date = '2024-09-30' AND care_processes_completed_count = 7
        AND NOT is_in_numerator AND latest_record_date IS NULL) <> 1
    OR COUNT_IF(reporting_date = '2024-10-31' AND care_processes_completed_count = 8
        AND is_in_numerator AND latest_record_date = '2024-09-01') <> 1
    OR COUNT_IF(reporting_date = '2025-10-31' AND care_processes_completed_count = 0
        AND NOT is_in_numerator AND latest_record_date IS NULL) <> 1
