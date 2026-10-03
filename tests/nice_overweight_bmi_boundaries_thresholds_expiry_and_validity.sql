{{ config(tags=['monthly-full', 'nice-history']) }}
{% set calculation = calculate_nice_bmi_register('overweight', reference='by_month', reference_dates="SELECT column1::DATE AS reference_date FROM VALUES ('2026-09-30'), ('2026-10-31')") %}
{% set calculation = calculation | replace(get_observations("'HEIGHT'") | string, 'SELECT * FROM synthetic_heights') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_int_nice_reference_population_by_month') %}
{% set calculation = calculation | replace(ref('int_bmi_all') | string, 'synthetic_int_bmi_all') %}
{% set calculation = calculation | replace(ref('int_ethnicity_qof_all') | string, 'synthetic_int_ethnicity_qof_all') %}

WITH synthetic_int_nice_reference_population_by_month AS (
SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date,
    IFF(people.column1 = -700 AND dates.column1 = '2026-09-30', 17, 18)::NUMBER AS age,
    '2008-10-01'::DATE AS birth_date_approx, 'Female'::VARCHAR AS gender,
    'SYNTHETIC'::VARCHAR AS practice_code, 'Synthetic practice'::VARCHAR AS practice_name
FROM VALUES (-730), (-729), (-728), (-727), (-726), (-725), (-724), (-723), (-722), (-721), (-720), (-719), (-718), (-717), (-716), (-715), (-714), (-713), (-712), (-711), (-710), (-709), (-708), (-707), (-706), (-705), (-704), (-703), (-702), (-701), (-700) AS people
CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
), synthetic_int_bmi_all AS (
SELECT column1::NUMBER AS person_id, column2::VARCHAR AS id,
    column3::TIMESTAMP_NTZ AS clinical_effective_date, column4::TIMESTAMP_NTZ AS date_recorded,
    column5::FLOAT AS bmi_value, column6::VARCHAR AS bmi_source
FROM VALUES (-730, 'A', '2026-09-30', NULL, 30, 'calculated'),
    (-700, 'A', '2026-09-30', NULL, 30, 'recorded'),
    (-701, 'A', '2025-09-30', NULL, 30, 'recorded'),
    (-702, 'A', '2025-10-01', NULL, 30, 'recorded'),
    (-703, 'A', '2026-09-30', NULL, 23, 'recorded'),
    (-704, 'A', '2026-09-30', NULL, 25, 'recorded'),
    (-705, 'A', '2026-09-30', NULL, 27.5, 'recorded'),
    (-706, 'A', '2026-09-30', NULL, 30, 'recorded'),
    (-707, 'A', '2026-09-30', NULL, 24.9, 'recorded'),
    (-708, 'A', '2026-08-01', NULL, 30, 'recorded'),
    (-708, 'Z', '2026-09-30', NULL, 151, 'recorded'),
    (-709, 'A', '2026-09-30', NULL, 9.9, 'recorded'),
    (-710, 'A', '2026-10-01', NULL, 30, 'recorded'),
    (-711, 'A', '2026-09-30', NULL, 29.9, 'recorded'),
    (-712, 'A', '2026-09-30', NULL, 27.4, 'recorded'),
    (-713, 'A', '2026-09-30', NULL, 30, 'calculated'),
    (-714, 'A', '2026-09-30', NULL, 23, 'recorded'),
    (-715, 'A', '2026-09-30', NULL, 24, 'recorded'),
    (-716, 'A', '2026-09-30', NULL, 28, 'recorded'),
    (-717, 'A', '2026-09-30', '2026-10-01', 30, 'recorded'),
    (-718, 'A', '2027-01-01', NULL, 30, 'recorded'),
    (-719, 'A', '2026-09-30', NULL, 35, 'recorded'),
    (-719, 'Z', '2026-09-30', NULL, 22, 'recorded'),
    (-720, 'A', '2026-09-30', NULL, 23, 'recorded'),
    (-721, 'A', '2026-09-30', NULL, 150.1, 'calculated'),
    (-722, 'A', '2026-09-30', NULL, 150, 'recorded'),
    (-723, 'A', '2026-09-30', NULL, 10, 'recorded'),
    (-724, 'A', '2026-09-30', NULL, 35, 'recorded'),
    (-725, 'A', '2026-09-30', NULL, 40, 'recorded'),
    (-726, 'A', '2026-09-30', NULL, 32.5, 'recorded'),
    (-727, 'A', '2026-09-30', NULL, 37.5, 'recorded'),
    (-728, 'A', '2026-09-30', NULL, 27.49, 'recorded'),
    (-729, 'A', '2026-09-30', NULL, 29.99, 'recorded')
), synthetic_int_ethnicity_qof_all AS (
SELECT column1::NUMBER AS person_id, column2::TIMESTAMP_NTZ AS clinical_effective_date,
    column3::TIMESTAMP_NTZ AS date_recorded, column4::BOOLEAN AS is_bame
FROM VALUES (-703, '2020-01-01', NULL, TRUE),
    (-705, '2020-01-01', NULL, TRUE),
    (-712, '2020-01-01', NULL, TRUE),
    (-715, '2026-10-01', NULL, TRUE),
    (-716, '2020-01-01', '2026-10-01', TRUE),
    (-720, '2020-01-01', NULL, TRUE),
    (-720, '2026-09-30', NULL, FALSE),
    (-726, '2020-01-01', NULL, TRUE),
    (-727, '2020-01-01', NULL, TRUE),
    (-728, '2020-01-01', NULL, TRUE)
), synthetic_heights AS (
SELECT column1::NUMBER AS person_id,column2::TIMESTAMP_NTZ AS clinical_effective_date,column3::TIMESTAMP_NTZ AS date_recorded, 'HEIGHT'::VARCHAR AS id, '180'::VARCHAR AS result_value,18::NUMBER AS age_at_event FROM VALUES (-713,'2026-08-01',NULL),(-721,'2026-08-01',NULL),(-730,'2026-08-01','2026-10-01'),(-706,'2026-08-01','2027-01-01')
), actual AS (
SELECT person_id, reference_date AS month_end_date, bmi_source, requires_lower_bmi_thresholds, bmi_category, bmi_risk_sort_key FROM ({{ calculation }})
), expected AS (
SELECT column1::NUMBER AS person_id, column2::DATE AS month_end_date, column3::VARCHAR AS bmi_source, column4::BOOLEAN AS requires_lower_bmi_thresholds, column5::VARCHAR AS bmi_category, column6::NUMBER AS bmi_risk_sort_key FROM VALUES (-729, '2026-09-30', 'recorded', FALSE, 'Overweight', 3),
    (-728, '2026-09-30', 'recorded', TRUE, 'Overweight', 3),
    (-727, '2026-09-30', 'recorded', TRUE, 'Obese Class III', 6),
    (-726, '2026-09-30', 'recorded', TRUE, 'Obese Class II', 5),
    (-725, '2026-09-30', 'recorded', FALSE, 'Obese Class III', 6),
    (-724, '2026-09-30', 'recorded', FALSE, 'Obese Class II', 5),
    (-722, '2026-09-30', 'recorded', FALSE, 'Obese Class III', 6),
    (-720, '2026-09-30', 'recorded', TRUE, 'Overweight', 3),
    (-716, '2026-09-30', 'recorded', FALSE, 'Overweight', 3),
    (-713, '2026-09-30', 'calculated', FALSE, 'Obese Class I', 4),
    (-712, '2026-09-30', 'recorded', TRUE, 'Overweight', 3),
    (-711, '2026-09-30', 'recorded', FALSE, 'Overweight', 3),
    (-706, '2026-09-30', 'recorded', FALSE, 'Obese Class I', 4),
    (-705, '2026-09-30', 'recorded', TRUE, 'Obese Class I', 4),
    (-704, '2026-09-30', 'recorded', FALSE, 'Overweight', 3),
    (-703, '2026-09-30', 'recorded', TRUE, 'Overweight', 3),
    (-702, '2026-09-30', 'recorded', FALSE, 'Obese Class I', 4),
    (-730, '2026-10-31', 'calculated', FALSE, 'Obese Class I', 4),
    (-729, '2026-10-31', 'recorded', FALSE, 'Overweight', 3),
    (-728, '2026-10-31', 'recorded', TRUE, 'Overweight', 3),
    (-727, '2026-10-31', 'recorded', TRUE, 'Obese Class III', 6),
    (-726, '2026-10-31', 'recorded', TRUE, 'Obese Class II', 5),
    (-725, '2026-10-31', 'recorded', FALSE, 'Obese Class III', 6),
    (-724, '2026-10-31', 'recorded', FALSE, 'Obese Class II', 5),
    (-722, '2026-10-31', 'recorded', FALSE, 'Obese Class III', 6),
    (-720, '2026-10-31', 'recorded', TRUE, 'Overweight', 3),
    (-717, '2026-10-31', 'recorded', FALSE, 'Obese Class I', 4),
    (-716, '2026-10-31', 'recorded', TRUE, 'Obese Class I', 4),
    (-715, '2026-10-31', 'recorded', TRUE, 'Overweight', 3),
    (-713, '2026-10-31', 'calculated', FALSE, 'Obese Class I', 4),
    (-712, '2026-10-31', 'recorded', TRUE, 'Overweight', 3),
    (-711, '2026-10-31', 'recorded', FALSE, 'Overweight', 3),
    (-710, '2026-10-31', 'recorded', FALSE, 'Obese Class I', 4),
    (-706, '2026-10-31', 'recorded', FALSE, 'Obese Class I', 4),
    (-705, '2026-10-31', 'recorded', TRUE, 'Obese Class I', 4),
    (-704, '2026-10-31', 'recorded', FALSE, 'Overweight', 3),
    (-703, '2026-10-31', 'recorded', TRUE, 'Overweight', 3),
    (-700, '2026-10-31', 'recorded', FALSE, 'Obese Class I', 4)
), actual_occurrences AS (SELECT person_id, month_end_date, bmi_source, requires_lower_bmi_thresholds, bmi_category, bmi_risk_sort_key, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_occurrences AS (SELECT person_id, month_end_date, bmi_source, requires_lower_bmi_thresholds, bmi_category, bmi_risk_sort_key, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS ((SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
UNION ALL (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences))
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
