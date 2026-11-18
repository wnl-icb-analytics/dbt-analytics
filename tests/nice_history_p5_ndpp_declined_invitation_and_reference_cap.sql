{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind171('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('NDH', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_referral_ndpp_all') | string, 'synthetic_referral') %}

WITH
synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        IFF(column1 = -9520, 17, 40) AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9508, '2024-09-30'), (-9508, '2024-10-31'), (-9509, '2024-09-30'),
        (-9510, '2024-09-30'), (-9511, '2024-09-30'), (-9520, '2024-09-30')
),
synthetic_register AS (
    SELECT person_id, reporting_date,
        '2024-09-01'::DATE AS earliest_diagnosis_date,
        person_id = -9511 AS has_unresolved_diabetes
    FROM synthetic_population
),
synthetic_referral AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS clinical_effective_date,
        column3::VARCHAR AS concept_code
    FROM VALUES (-9508, '2024-10-01', '1025301000000100'),
        (-9509, '2024-09-01', '1025321000000109'),
        (-9510, '2024-09-30', 'SYNTHETIC_INVITATION'),
        (-9511, '2024-09-30', '1025321000000109'),
        (-9520, '2024-09-30', '1025321000000109')
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 4
    OR COUNT_IF(person_id = -9508 AND reporting_date = '2024-09-30' AND NOT is_in_numerator) <> 1
    OR COUNT_IF(person_id = -9508 AND reporting_date = '2024-10-31' AND is_in_numerator
        AND latest_record_date = '2024-10-01') <> 1
    OR COUNT_IF(person_id = -9509 AND is_in_numerator AND latest_record_date = diagnosis_date) <> 1
    OR COUNT_IF(person_id = -9510 AND is_in_numerator) <> 0
