{{ config(tags=['monthly-full', 'nice-history']) }}
{% set calculation = calculate_nice_bmi_register('overweight', reference='by_month', reference_dates="SELECT column1::DATE AS reference_date FROM VALUES ('2026-09-30'), ('2026-10-31')") %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_int_nice_reference_population_by_month') %}
{% set calculation = calculation | replace(ref('int_nice_weight_profile_by_month') | string, 'synthetic_int_nice_weight_profile_by_month') %}

WITH synthetic_int_nice_reference_population_by_month AS (
SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date,
    IFF(people.column1 = -640, 17, 18)::NUMBER AS age,
    '2008-01-15'::DATE AS birth_date_approx, 'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code, 'Synthetic practice'::VARCHAR AS practice_name
FROM VALUES (-640), (-641), (-642), (-643), (-644), (-645), (-646), (-647), (-648), (-649), (-650), (-651), (-652) AS people
CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
),
synthetic_int_nice_weight_profile_by_month AS (
SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date,
    people.column2::DATE AS bmi_date, people.column3::FLOAT AS bmi_value,
    people.column4::BOOLEAN AS is_recorded_white, NULL::DATE AS latest_weight_advice_date
FROM VALUES (-640, '2026-09-30', 30, FALSE), (-641, '2025-09-30', 30, FALSE),
    (-642, '2025-10-01', 30, FALSE), (-643, '2026-09-30', 23, FALSE),
    (-644, '2026-09-30', 25, TRUE), (-645, '2026-09-30', 27.5, FALSE),
    (-646, '2026-09-30', 30, TRUE), (-647, '2026-09-30', 24.9, TRUE),
    (-648, '2026-09-30', 401, FALSE), (-649, '2026-09-30', 0, FALSE),
    (-650, '2026-10-01', 30, FALSE), (-651, '2026-09-30', 29.9, TRUE),
    (-652, '2026-09-30', 27.4, FALSE) AS people
CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
),
actual AS (
SELECT person_id, reference_date AS month_end_date, bmi_threshold
FROM ({{ calculation }})
),
expected AS (
SELECT column1::NUMBER AS person_id, column2::DATE AS month_end_date, column3::FLOAT AS bmi_threshold FROM VALUES (-643, '2026-09-30', 23),
    (-644, '2026-09-30', 25),
    (-645, '2026-09-30', 23),
    (-646, '2026-09-30', 25),
    (-651, '2026-09-30', 25),
    (-652, '2026-09-30', 23),
    (-642, '2026-09-30', 23),
    (-643, '2026-10-31', 23),
    (-644, '2026-10-31', 25),
    (-645, '2026-10-31', 23),
    (-646, '2026-10-31', 25),
    (-651, '2026-10-31', 25),
    (-652, '2026-10-31', 23),
    (-650, '2026-10-31', 23)
),
actual_occurrences AS (SELECT person_id, month_end_date, bmi_threshold, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_occurrences AS (SELECT person_id, month_end_date, bmi_threshold, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS ((SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
UNION ALL (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences))
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
