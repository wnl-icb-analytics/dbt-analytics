{{ config(tags=['monthly-full', 'nice-history']) }}
{% set calculation = calculate_aki_register(reference='by_month', reference_dates="SELECT column1::DATE AS reference_date FROM VALUES ('2026-09-30'), ('2026-10-31')") %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_int_nice_reference_population_by_month') %}
{% set calculation = calculation | replace(ref('int_aki_diagnoses_all') | string, 'synthetic_int_aki_diagnoses_all') %}

WITH synthetic_int_nice_reference_population_by_month AS (
SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date,
    CASE WHEN people.column1 = -602 AND dates.column1 = '2026-09-30' THEN 17 ELSE 18 END AS age,
    '2008-10-01'::DATE AS birth_date_approx, 'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code, 'Synthetic practice'::VARCHAR AS practice_name
FROM VALUES (-601), (-602), (-603), (-604), (-605), (-606), (-607), (-608), (-609) AS people
CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
WHERE NOT (people.column1 = -608 AND dates.column1 = '2026-10-31')
),
synthetic_int_aki_diagnoses_all AS (
SELECT column1::NUMBER AS person_id, column2::TIMESTAMP_NTZ AS clinical_effective_date,
    column3::TIMESTAMP_NTZ AS date_recorded
FROM VALUES (-601, '2026-09-30', '2026-10-01'), (-602, '2020-01-01', NULL),
    (-603, '2026-10-01', NULL), (-604, '2020-01-01', NULL), (-605, '2027-01-01', NULL),
    (-606, '2020-01-01', NULL), (-606, '2020-01-01', NULL), (-607, '2026-09-30', '2026-09-30'),
    (-608, '2020-01-01', NULL), (-609, '2020-01-01', '2026-10-01'), (-609, '2026-09-01', NULL)
),
actual AS (
SELECT person_id, reference_date AS month_end_date, earliest_diagnosis_date, latest_diagnosis_date
FROM ({{ calculation }})
),
expected AS (
SELECT column1::NUMBER AS person_id, column2::DATE AS month_end_date, column3::DATE AS earliest_diagnosis_date, column4::DATE AS latest_diagnosis_date FROM VALUES (-604, '2026-09-30', '2020-01-01', '2020-01-01'),
    (-606, '2026-09-30', '2020-01-01', '2020-01-01'),
    (-607, '2026-09-30', '2026-09-30', '2026-09-30'),
    (-608, '2026-09-30', '2020-01-01', '2020-01-01'),
    (-609, '2026-09-30', '2026-09-01', '2026-09-01'),
    (-601, '2026-10-31', '2026-09-30', '2026-09-30'),
    (-602, '2026-10-31', '2020-01-01', '2020-01-01'),
    (-603, '2026-10-31', '2026-10-01', '2026-10-01'),
    (-604, '2026-10-31', '2020-01-01', '2020-01-01'),
    (-606, '2026-10-31', '2020-01-01', '2020-01-01'),
    (-607, '2026-10-31', '2026-09-30', '2026-09-30'),
    (-609, '2026-10-31', '2020-01-01', '2026-09-01')
),
actual_occurrences AS (SELECT person_id, month_end_date, earliest_diagnosis_date, latest_diagnosis_date, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_occurrences AS (SELECT person_id, month_end_date, earliest_diagnosis_date, latest_diagnosis_date, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS ((SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
UNION ALL (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences))
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
