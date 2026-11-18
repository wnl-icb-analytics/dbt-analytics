{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind81('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('DM', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_foot_examination_all') | string, 'synthetic_foot') %}

WITH
synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        40 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9501, '2024-09-30'), (-9501, '2024-10-31'), (-9502, '2024-09-30'), (-9502, '2024-10-31'), (-9503, '2024-09-30'), (-9503, '2024-10-31')
),
synthetic_register AS (
    SELECT person_id, reporting_date
    FROM synthetic_population
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
    FROM VALUES
        (-9501, '2024-09-01', TRUE, FALSE, TRUE, '2024-10-01', NULL),
        (-9502, '2024-09-01', TRUE, TRUE, TRUE, NULL, '2024-10-01'),
        (-9503, '2023-06-30', TRUE, TRUE, TRUE, NULL, NULL)
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 5
    OR COUNT_IF(person_id = -9501 AND reporting_date = '2024-09-30' AND NOT is_in_numerator) <> 1
    OR COUNT_IF(person_id = -9501 AND reporting_date = '2024-10-31' AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = -9502 AND reporting_date = '2024-09-30' AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = -9502 AND reporting_date = '2024-10-31') <> 0
    OR COUNT_IF(person_id = -9503 AND reporting_date = '2024-09-30' AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = -9503 AND reporting_date = '2024-10-31' AND NOT is_in_numerator
        AND latest_record_date IS NULL AND latest_foot_risk_category IS NULL) <> 1
