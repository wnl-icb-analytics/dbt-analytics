{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_249 = nice_ind249('by_month') %}
{% set actual_249 = actual_249 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_249 = actual_249 | replace(nice_register('DM', 'by_month'), 'SELECT * FROM synthetic_dm') %}
{% set actual_249 = actual_249 | replace(nice_register('FRAIL', 'by_month'), 'SELECT * FROM synthetic_frail') %}
{% set actual_249 = actual_249 | replace(nice_ref('int_nice_blood_pressure_latest', 'by_month') | string, 'synthetic_bp') %}

WITH synthetic_keys AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age
    FROM VALUES
        (612, '2024-01-31', 17),
        (612, '2024-02-29', 17),
        (613, '2024-01-31', 79),
        (614, '2024-01-31', 40),
        (615, '2024-01-31', 80)
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
        (612, '2024-01-31'),
        (612, '2024-02-29'),
        (613, '2024-01-31'),
        (614, '2024-01-31'),
        (615, '2024-01-31')
),

synthetic_frail AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_frailty_severity
    FROM VALUES
        (0, '2024-01-31', NULL)
),

synthetic_bp AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::DATE AS latest_bp_date,
        column4::BOOLEAN AS is_valid_bp,
        column5::NUMBER AS latest_systolic_value,
        column6::NUMBER AS latest_diastolic_value,
        column7::BOOLEAN AS is_home_bp_event,
        column8::BOOLEAN AS is_abpm_bp_event,
        column9::VARCHAR AS applied_measurement_context
    FROM VALUES
        (612, '2024-01-31', '2023-01-31', TRUE, 139, 89, FALSE, FALSE, 'CLINIC'),
        (612, '2024-02-29', '2023-01-31', TRUE, 139, 89, FALSE, FALSE, 'CLINIC'),
        (613, '2024-01-31', '2024-01-01', TRUE, 140, 89, FALSE, FALSE, 'CLINIC'),
        (614, '2024-01-31', '2024-01-01', TRUE, 134, 85, TRUE, FALSE, 'HBPM_ABPM')
),

actual_249 AS ({{ actual_249 }})

SELECT COUNT(*) AS failure_count
FROM actual_249
HAVING COUNT(*) <> 4
    OR COUNT_IF(person_id = 612 AND reporting_date = '2024-01-31' AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = 612 AND reporting_date = '2024-02-29'
        AND indicator_status = 'NOT_RECORDED_IN_PERIOD' AND latest_record_date IS NULL
        AND latest_bp_date = '2023-01-31' AND latest_systolic_value = 139) <> 1
    OR COUNT_IF(person_id IN (613,614) AND indicator_status = 'ABOVE_TARGET') <> 2
