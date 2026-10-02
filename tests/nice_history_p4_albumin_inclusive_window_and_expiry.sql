{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind144('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation = calculation | replace(nice_register('CKD', 'by_month') | trim, 'SELECT person_id, reporting_date FROM synthetic_population') %}
{% set calculation = calculation | replace(ref('int_urine_acr_all') | string, 'synthetic_albumin') %}

WITH synthetic_keys AS (
    SELECT -9401 AS person_id, column1::DATE AS reporting_date
    FROM VALUES ('2026-09-30'), ('2026-10-31')
),
synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        60 AS age,
        '1966-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM synthetic_keys
),
synthetic_albumin AS (
    SELECT
        -9401 AS person_id,
        column1::TIMESTAMP_NTZ AS clinical_effective_date,
        column2::VARCHAR AS albumin_test_type
    FROM VALUES
        ('2025-09-30', 'PCR'),
        ('2026-10-01', 'ALBUMIN'),
        ('2026-11-01', 'ACR')
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2026-09-30'
        AND latest_record_date = '2025-09-30' AND is_in_numerator) <> 1
    OR COUNT_IF(reporting_date = '2026-10-31'
        AND latest_record_date IS NULL AND NOT is_in_numerator
        AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 1
