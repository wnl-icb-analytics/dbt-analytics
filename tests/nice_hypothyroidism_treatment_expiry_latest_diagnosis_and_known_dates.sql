{{ config(tags=['monthly-full', 'nice-history']) }}
{% set calculation = calculate_nice_hypothyroidism_register(reference='by_month', reference_dates="SELECT column1::DATE AS reference_date FROM VALUES ('2026-09-30'), ('2026-10-31')") %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_int_nice_reference_population_by_month') %}
{% set calculation = calculation | replace(ref('int_hypothyroidism_diagnoses_all') | string, 'synthetic_int_hypothyroidism_diagnoses_all') %}
{% set calculation = calculation | replace(ref('int_subclinical_hypothyroidism_diagnoses_all') | string, 'synthetic_int_subclinical_hypothyroidism_diagnoses_all') %}
{% set calculation = calculation | replace(ref('int_levothyroxine_medications_all') | string, 'synthetic_int_levothyroxine_medications_all') %}

WITH synthetic_int_nice_reference_population_by_month AS (
SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date, 50::NUMBER AS age, '1976-01-15'::DATE AS birth_date_approx, 'Female'::VARCHAR AS gender, 'SYNTHETIC'::VARCHAR AS practice_code, 'Synthetic practice'::VARCHAR AS practice_name FROM VALUES (-621), (-622), (-623), (-624), (-625), (-626), (-627), (-628), (-629), (-630), (-631), (-632) AS people CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
),
synthetic_int_hypothyroidism_diagnoses_all AS (
SELECT column1::NUMBER AS person_id, column2::VARCHAR AS id,
    column3::TIMESTAMP_NTZ AS clinical_effective_date, column4::TIMESTAMP_NTZ AS date_recorded,
    TRUE AS is_diagnosis_code
FROM VALUES (-621, 'A', '2020-01-01', NULL), (-622, 'A', '2020-01-01', NULL),
    (-623, 'A', '2020-01-01', NULL), (-624, 'A', '2020-01-01', NULL),
    (-625, 'A', '2020-01-01', NULL), (-626, 'A', '2020-01-01', '2026-10-01'),
    (-627, 'A', '2026-08-01', NULL), (-627, 'Z', '2026-09-01', NULL),
    (-628, 'A', '2026-08-01', NULL), (-628, 'Z', '2026-10-01', NULL),
    (-629, 'Z', '2026-09-01', NULL), (-630, 'A', '2026-09-01', NULL),
    (-631, 'Z', '2026-09-01', NULL), (-632, 'A', '2026-09-01', NULL)
),
synthetic_int_subclinical_hypothyroidism_diagnoses_all AS (
SELECT column1::NUMBER AS person_id, column2::VARCHAR AS id,
    column3::TIMESTAMP_NTZ AS clinical_effective_date, column4::TIMESTAMP_NTZ AS date_recorded
FROM VALUES (-627, 'A', '2026-08-01', NULL), (-628, 'Z', '2026-10-01', NULL),
    (-629, 'A', '2026-09-01', NULL), (-630, 'Z', '2026-09-01', NULL),
    (-631, 'A', '2026-08-01', '2026-10-01'), (-632, 'Z', '2026-09-30', '2026-10-01')
),
synthetic_int_levothyroxine_medications_all AS (
SELECT column1::NUMBER AS person_id, column2::DATE AS order_date,
    column3::DATE AS date_recorded
FROM VALUES (-621, '2026-03-30', NULL), (-622, '2026-03-31', NULL),
    (-623, '2026-09-30', NULL), (-623, '2026-09-30', NULL),
    (-624, '2026-10-01', NULL), (-625, '2026-09-30', '2026-10-01'),
    (-626, '2026-09-30', NULL), (-627, '2026-09-30', NULL),
    (-628, '2026-09-30', NULL), (-629, '2026-09-30', NULL),
    (-630, '2026-09-30', NULL), (-631, '2026-09-30', NULL), (-632, '2026-09-30', NULL)
),
actual AS (
SELECT person_id, reference_date AS month_end_date, latest_levothyroxine_order_date
FROM ({{ calculation }})
),
expected AS (
SELECT column1::NUMBER AS person_id, column2::DATE AS month_end_date, column3::DATE AS latest_levothyroxine_order_date FROM VALUES (-632, '2026-09-30', '2026-09-30'),
    (-622, '2026-09-30', '2026-03-31'),
    (-623, '2026-09-30', '2026-09-30'),
    (-627, '2026-09-30', '2026-09-30'),
    (-628, '2026-09-30', '2026-09-30'),
    (-629, '2026-09-30', '2026-09-30'),
    (-631, '2026-09-30', '2026-09-30'),
    (-623, '2026-10-31', '2026-09-30'),
    (-624, '2026-10-31', '2026-10-01'),
    (-625, '2026-10-31', '2026-09-30'),
    (-626, '2026-10-31', '2026-09-30'),
    (-627, '2026-10-31', '2026-09-30'),
    (-629, '2026-10-31', '2026-09-30'),
    (-631, '2026-10-31', '2026-09-30')
),
actual_occurrences AS (SELECT person_id, month_end_date, latest_levothyroxine_order_date, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_occurrences AS (SELECT person_id, month_end_date, latest_levothyroxine_order_date, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS ((SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
UNION ALL (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences))
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
