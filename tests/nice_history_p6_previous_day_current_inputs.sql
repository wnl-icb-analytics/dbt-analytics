{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_135 = nice_ind135('current') %}
{% set actual_135 = actual_135 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_135 = actual_135 | replace(nice_register('DM', 'current'), 'SELECT * FROM synthetic_dm') %}
{% set actual_135 = actual_135 | replace(nice_register('FRAIL', 'current'), 'SELECT * FROM synthetic_frail') %}
{% set actual_135 = actual_135 | replace(ref('int_nice_hba1c_evidence') | string, 'synthetic_hba') %}
{% set actual_249 = nice_ind249('current') %}
{% set actual_249 = actual_249 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_249 = actual_249 | replace(nice_register('DM', 'current'), 'SELECT * FROM synthetic_dm') %}
{% set actual_249 = actual_249 | replace(nice_register('FRAIL', 'current'), 'SELECT * FROM synthetic_frail') %}
{% set actual_249 = actual_249 | replace(ref('int_nice_blood_pressure_latest') | string, 'synthetic_bp') %}

WITH synthetic_keys AS (SELECT 616::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date, 40::NUMBER AS age),

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

synthetic_dm AS (SELECT person_id, reporting_date FROM synthetic_keys),

synthetic_frail AS (SELECT 0::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date, NULL::VARCHAR AS latest_frailty_severity),

synthetic_hba AS (SELECT 616::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date, 'a'::VARCHAR AS latest_hba1c_observation_id, reporting_date AS latest_hba1c_date, 64::NUMBER AS latest_hba1c_value, TRUE AS is_latest_hba1c_valid, NULL::DATE AS latest_fructosamine_date, NULL::DATE AS latest_dmmax_date),

synthetic_bp AS (SELECT 616::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date, reporting_date AS latest_bp_date, TRUE AS is_valid_bp, 139::NUMBER AS latest_systolic_value, 89::NUMBER AS latest_diastolic_value, FALSE AS is_home_bp_event, FALSE AS is_abpm_bp_event, 'CLINIC'::VARCHAR AS applied_measurement_context),

actual_135 AS ({{ actual_135 }}),

actual_249 AS ({{ actual_249 }}),

actual AS (SELECT indicator_id, reporting_date, is_in_numerator FROM actual_135 UNION ALL SELECT indicator_id, reporting_date, is_in_numerator FROM actual_249)

SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 2
