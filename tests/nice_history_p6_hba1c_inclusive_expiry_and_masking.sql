{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_135 = nice_ind135('by_month') %}
{% set actual_135 = actual_135 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_135 = actual_135 | replace(nice_register('DM', 'by_month'), 'SELECT * FROM synthetic_dm') %}
{% set actual_135 = actual_135 | replace(nice_register('FRAIL', 'by_month'), 'SELECT * FROM synthetic_frail') %}
{% set actual_135 = actual_135 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}

WITH synthetic_keys AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age
    FROM VALUES
        (601, '2024-01-31', 40),
        (601, '2024-02-29', 40)
),

synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        age,
        '1980-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM synthetic_keys
),

synthetic_dm AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date
    FROM VALUES
        (601, '2024-01-31'),
        (601, '2024-02-29')
),

synthetic_frail AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_frailty_severity
    FROM VALUES
        (0, '2024-01-31', NULL)
),

synthetic_hba AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_hba1c_observation_id,
        column4::DATE AS latest_hba1c_date,
        column5::NUMBER AS latest_hba1c_value,
        column6::BOOLEAN AS is_latest_hba1c_valid,
        column7::DATE AS latest_fructosamine_date,
        column8::DATE AS latest_dmmax_date
    FROM VALUES
        (601, '2024-01-31', 'test', '2023-01-31', 64, TRUE, NULL, NULL),
        (601, '2024-02-29', 'test', '2023-01-31', 64, TRUE, NULL, NULL)
),

actual_135 AS ({{ actual_135 }})

SELECT COUNT(*) AS failure_count
FROM actual_135
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2024-01-31' AND indicator_status = 'ACHIEVED'
        AND latest_hba1c_value = 64 AND latest_record_date = '2023-01-31') <> 1
    OR COUNT_IF(reporting_date = '2024-02-29' AND indicator_status = 'NOT_RECORDED_IN_PERIOD'
        AND latest_hba1c_observation_id IS NULL AND latest_record_date IS NULL
        AND latest_hba1c_value IS NULL AND NOT is_latest_hba1c_valid) <> 1
