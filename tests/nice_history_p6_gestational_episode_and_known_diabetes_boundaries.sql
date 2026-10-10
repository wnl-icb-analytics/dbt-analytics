{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_173 = nice_ind173('by_month') %}
{% set actual_173 = actual_173 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_173 = actual_173 | replace(nice_register('GESTDIAB', 'by_month'), 'SELECT * FROM synthetic_gestdiab') %}
{% set actual_173 = actual_173 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}
{% set actual_173 = actual_173 | replace(ref('int_diabetes_diagnoses_all') | string, 'synthetic_diagnoses') %}

WITH synthetic_keys AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age
    FROM VALUES
        (609, '2024-01-31', 40),
        (609, '2024-02-29', 40),
        (610, '2024-01-31', 40),
        (610, '2024-02-29', 40),
        (611, '2024-01-31', 40),
        (611, '2024-02-29', 40)
),

synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        age,
        '1980-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM synthetic_keys
),

synthetic_gestdiab AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::TIMESTAMP_NTZ AS latest_diagnosis_date
    FROM VALUES
        (609, '2024-01-31', '2023-01-31'),
        (609, '2024-02-29', '2023-01-31'),
        (610, '2024-01-31', '2020-01-01'),
        (610, '2024-02-29', '2020-01-01'),
        (611, '2024-01-31', '2020-01-01'),
        (611, '2024-02-29', '2020-01-01')
),

synthetic_hba AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_hba1c_observation_id,
        column4::DATE AS latest_hba1c_date,
        column5::NUMBER AS latest_hba1c_value,
        column6::BOOLEAN AS is_latest_hba1c_valid,
        column7::DATE AS latest_fructosamine_date,
        column8::DATE AS latest_dmmax_date
    FROM VALUES
        (609, '2024-02-29', 'a', '2024-02-01', NULL, FALSE, NULL, NULL),
        (610, '2024-01-31', 'b', '2024-01-01', NULL, FALSE, NULL, NULL),
        (610, '2024-02-29', 'b', '2024-01-01', NULL, FALSE, NULL, NULL),
        (611, '2024-01-31', 'c', '2024-01-01', NULL, FALSE, NULL, NULL),
        (611, '2024-02-29', 'c', '2024-01-01', NULL, FALSE, NULL, NULL)
),

synthetic_diagnoses AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::TIMESTAMP_NTZ AS date_recorded,
        column4::BOOLEAN AS is_diagnosis_code,
        FALSE AS is_resolved_code
    FROM VALUES
        (610, '2022-01-01', '2024-02-15', TRUE),
        (611, '2023-01-31', NULL, TRUE),
        (611, '2024-01-01', NULL, FALSE)
),

actual_173 AS ({{ actual_173 }})

SELECT COUNT(*) AS failure_count
FROM actual_173
HAVING COUNT(*) <> 3
    OR COUNT_IF(person_id = 609 AND reporting_date = '2024-02-29' AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = 610 AND reporting_date = '2024-01-31' AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = 611 AND reporting_date = '2024-01-31' AND is_in_numerator) <> 1
