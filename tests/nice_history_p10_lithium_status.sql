{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation87 = nice_ind87('by_month') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_physical_health_evidence_by_month') | string, 'synthetic_physical') %}
WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        40 AS age, 'Female' AS gender, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name, '1984-01-01'::DATE AS birth_date_approx
    FROM VALUES (-10101,'2024-05-31'), (-10101,'2024-06-30'),
        (-10102,'2024-05-31'), (-10103,'2024-05-31'),
        (-10104,'2024-05-31'), (-10105,'2024-05-31')
),
synthetic_ltc AS (
    SELECT person_id, reporting_date, person_id <> -10105 AS is_on_lithium,
        FALSE AS has_active_smi_diagnosis
    FROM synthetic_population
),
synthetic_physical AS (
    SELECT person_id, reporting_date, '2024-01-31'::DATE AS latest_lithium_level_date,
        CASE person_id WHEN -10101 THEN 0.4 WHEN -10102 THEN 1.0
            WHEN -10103 THEN 1.01 ELSE NULL END::FLOAT AS latest_lithium_level,
        person_id IN (-10101,-10102) AS is_latest_lithium_level_in_range
    FROM synthetic_population
),
actual87 AS ({{ calculation87 }})
SELECT COUNT(*) AS failure_count
FROM actual87
HAVING COUNT(*) <> 5
    OR COUNT_IF(indicator_status = 'ACHIEVED' AND is_in_numerator
        AND latest_record_date = '2024-01-31') <> 2
    OR COUNT_IF(person_id = -10101 AND reporting_date = '2024-06-30'
        AND indicator_status = 'NOT_RECORDED_IN_PERIOD' AND NOT is_in_numerator
        AND latest_record_date IS NULL AND latest_lithium_level = 0.4
        AND latest_lithium_level_date = '2024-01-31') <> 1
    OR COUNT_IF(person_id = -10103 AND indicator_status = 'OUT_OF_RANGE'
        AND NOT is_in_numerator AND latest_record_date = '2024-01-31') <> 1
    OR COUNT_IF(person_id = -10104 AND indicator_status = 'NOT_ASSESSABLE'
        AND NOT is_in_numerator AND latest_record_date = '2024-01-31') <> 1
