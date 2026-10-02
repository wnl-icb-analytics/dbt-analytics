{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind137('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('DM', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_retinal_screening_all') | string, 'synthetic_events') %}

WITH
synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        40 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9512, '2024-09-30'), (-9512, '2024-10-31')
),
synthetic_register AS (
    SELECT person_id, reporting_date
    FROM synthetic_population
),
synthetic_events AS (
    -- No numeric result is supplied: these are recording checks.
    SELECT -9512::NUMBER AS person_id, column1::DATE AS clinical_effective_date
    FROM VALUES ('2023-09-30'), ('2024-11-01')
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2024-09-30' AND is_in_numerator
        AND latest_record_date = '2023-09-30') <> 1
    OR COUNT_IF(reporting_date = '2024-10-31' AND NOT is_in_numerator AND latest_record_date IS NULL) <> 1
