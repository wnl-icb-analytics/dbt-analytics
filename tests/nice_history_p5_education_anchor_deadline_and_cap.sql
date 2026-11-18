{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind88('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('DM', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_diabetes_structured_education_all') | string, 'synthetic_education') %}

WITH
synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        40 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9504, '2024-09-30'), (-9504, '2024-10-31'), (-9505, '2024-09-30'),
        (-9506, '2024-09-30'), (-9507, '2024-09-30'),
        (-9518, '2024-09-30'), (-9518, '2024-10-31')
),
synthetic_register AS (
    SELECT person_id, reporting_date,
        CASE
            WHEN person_id = -9507 THEN '2023-09-30'
            WHEN person_id = -9518 THEN '2024-09-01'
            ELSE '2023-12-30'
        END::DATE AS earliest_diagnosis_date
    FROM synthetic_population
),
synthetic_education AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS clinical_effective_date,
        column3::VARCHAR AS record_type
    FROM VALUES (-9504, '2024-10-01', 'REFERRED'),
        (-9505, '2024-09-30', 'REFERRED'),
        (-9506, '2024-09-30', 'ATTENDED'),
        (-9507, '2023-09-30', 'REFERRED'),
        (-9518, '2024-10-01', 'REFERRED')
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 5
    OR COUNT_IF(person_id = -9504 AND is_in_numerator) <> 0
    OR COUNT_IF(person_id = -9505 AND is_in_numerator AND latest_record_date = '2024-09-30') <> 1
    OR COUNT_IF(person_id = -9506 AND is_in_numerator) <> 0
    OR COUNT_IF(person_id = -9507 AND is_in_numerator AND latest_record_date = diagnosis_date) <> 1
    OR COUNT_IF(person_id = -9518 AND reporting_date = '2024-09-30') <> 0
    OR COUNT_IF(person_id = -9518 AND reporting_date = '2024-10-31'
        AND is_in_numerator AND latest_record_date = '2024-10-01') <> 0
