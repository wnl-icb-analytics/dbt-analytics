{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation_235 = nice_ind235('by_month') %}
{% set calculation_235 = calculation_235 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_235 = calculation_235 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}
{% set calculation_235 = calculation_235 | replace(nice_ref('int_nice_blood_pressure_control_latest_by_context', 'by_month') | string, '(SELECT * FROM synthetic_bp_context)') %}
{% set calculation_264 = nice_ind264('by_month') %}
{% set calculation_264 = calculation_264 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_264 = calculation_264 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}
{% set calculation_264 = calculation_264 | replace(nice_ref('int_nice_blood_pressure_control_latest_by_context', 'by_month') | string, '(SELECT * FROM synthetic_bp_context)') %}

WITH synthetic_keys AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date
    FROM VALUES
        (-9403, '2026-09-30'), (-9403, '2026-10-31'),
        (-9404, '2026-09-30'), (-9404, '2026-10-31')
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
synthetic_profile AS (
    SELECT
        person_id,
        reporting_date,
        IFF(person_id = -9403, 69, 70)::FLOAT AS latest_acr_value,
        NULL::VARCHAR AS latest_frailty_severity
    FROM synthetic_keys
),
synthetic_bp AS (
    SELECT
        person_id,
        reporting_date,
        '2025-09-30'::DATE AS latest_bp_date,
        TRUE AS is_valid_bp,
        120::NUMBER AS latest_systolic_value,
        70::NUMBER AS latest_diastolic_value,
        'CLINIC' AS applied_measurement_context
    FROM synthetic_keys
),
synthetic_bp_context AS (
    SELECT person_id, reporting_date AS reporting_date, latest_bp_date AS reading_date,
        latest_systolic_value AS systolic_value, latest_diastolic_value AS diastolic_value,
        is_valid_bp, FALSE AS is_home_bp_event, FALSE AS is_abpm_bp_event, applied_measurement_context,
        IFF(is_home_bp_event, 'HOME', IFF(is_abpm_bp_event, 'ABPM', 'CLINIC')) AS bp_context_group,
        NULL::TIMESTAMP_NTZ AS date_recorded, 'SYN' AS observation_id
    FROM synthetic_bp
),
actual AS (
    SELECT indicator_id, reporting_date, latest_bp_date, latest_record_date, is_in_numerator, indicator_status
    FROM ({{ calculation_235 }})
    UNION ALL
    SELECT indicator_id, reporting_date, latest_bp_date, latest_record_date, is_in_numerator, indicator_status
    FROM ({{ calculation_264 }})
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 4
    OR COUNT_IF(reporting_date = '2026-09-30'
        AND latest_record_date = '2025-09-30' AND is_in_numerator) <> 2
    OR COUNT_IF(reporting_date = '2026-10-31'
        AND latest_bp_date = '2025-09-30' AND latest_record_date IS NULL
        AND NOT is_in_numerator AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 2
