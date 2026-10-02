{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind134('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('DM', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_proteinuria_all') | string, 'synthetic_proteinuria') %}
{% set calculation = calculation | replace(ref('int_ras_contraindication_all') | string, 'synthetic_contra') %}
{% set calculation = calculation | replace(ref('int_nice_therapy_evidence_by_month') | string, 'synthetic_therapy') %}

WITH
synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        40 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9513, '2024-09-30'), (-9513, '2024-10-31'), (-9514, '2024-09-30'), (-9514, '2024-10-31'), (-9515, '2024-09-30'), (-9515, '2024-10-31'),
        (-9521, '2024-09-30'), (-9521, '2024-10-31')
),
synthetic_register AS (
    SELECT person_id, reporting_date
    FROM synthetic_population
),
synthetic_proteinuria AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS clinical_effective_date,
        'PRT_COD' AS source_cluster_id
    FROM VALUES (-9513, '2020-01-01'), (-9514, '2020-01-01'), (-9515, '2024-10-01'),
        (-9521, '2020-01-01')
),
synthetic_contra AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS clinical_effective_date,
        column3::VARCHAR AS drug_class, column4::BOOLEAN AS is_persisting
    FROM VALUES (-9513, '2020-01-01', 'ACE_INHIBITOR', TRUE),
        (-9513, '2023-09-30', 'ARB', FALSE),
        (-9514, '2024-10-01', 'ACE_INHIBITOR', TRUE),
        (-9514, '2024-10-01', 'ARB', TRUE)
),
synthetic_therapy AS (
    SELECT person_id, reporting_date,
        '2024-03-30'::DATE AS latest_ras_order_date,
        'ACE_INHIBITOR' AS latest_ras_class
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 5
    OR COUNT_IF(person_id = -9513 AND reporting_date = '2024-09-30') <> 0
    OR COUNT_IF(person_id = -9513 AND reporting_date = '2024-10-31'
        AND indicator_status = 'NOT_TREATED_IN_PERIOD' AND latest_record_date IS NULL) <> 1
    OR COUNT_IF(person_id = -9514 AND reporting_date = '2024-09-30'
        AND is_in_numerator AND latest_record_date = '2024-03-30') <> 1
    OR COUNT_IF(person_id = -9514 AND reporting_date = '2024-10-31') <> 0
    OR COUNT_IF(person_id = -9515 AND reporting_date = '2024-09-30') <> 0
    OR COUNT_IF(person_id = -9515 AND reporting_date = '2024-10-31') <> 1
    OR COUNT_IF(person_id = -9521 AND reporting_date = '2024-09-30'
        AND is_in_numerator AND latest_record_date = '2024-03-30') <> 1
    OR COUNT_IF(person_id = -9521 AND reporting_date = '2024-10-31'
        AND indicator_status = 'NOT_TREATED_IN_PERIOD' AND latest_record_date IS NULL
        AND latest_therapy_order_date = '2024-03-30') <> 1
