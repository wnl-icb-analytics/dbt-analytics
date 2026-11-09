{{ config(tags=['monthly-full', 'nice-history']) }}

{% set current_input = nice_ref('int_nice_blood_pressure_latest', 'current') %}
{% set current_input = current_input | replace(ref('int_nice_blood_pressure_latest') | string, 'synthetic_current') %}
{% set monthly_input = nice_ref('int_nice_blood_pressure_latest', 'by_month') %}
{% set monthly_input = monthly_input | string | replace(ref('int_nice_blood_pressure_latest_by_month') | string, 'synthetic_monthly') %}

-- A consumer-only rebuild retains yesterday's current input; monthly dates remain unchanged.
WITH synthetic_current AS (
    SELECT
        -9101::NUMBER AS person_id,
        DATEADD(day, -2, CURRENT_DATE())::DATE AS latest_bp_date,
        130::FLOAT AS latest_systolic_value,
        80::FLOAT AS latest_diastolic_value,
        TRUE AS is_valid_bp,
        FALSE AS is_home_bp_event,
        FALSE AS is_abpm_bp_event,
        'CLINIC'::VARCHAR AS applied_measurement_context,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date
),

synthetic_monthly AS (
    SELECT
        -9101::NUMBER AS person_id,
        column1::DATE AS latest_bp_date,
        130::FLOAT AS latest_systolic_value,
        80::FLOAT AS latest_diastolic_value,
        TRUE AS is_valid_bp,
        FALSE AS is_home_bp_event,
        FALSE AS is_abpm_bp_event,
        'CLINIC'::VARCHAR AS applied_measurement_context,
        column1::DATE AS reporting_date
    FROM VALUES ('2026-08-31'), ('2026-09-30')
),

current_consumer AS (
    SELECT profile.*
    FROM {{ current_input }} AS profile
    WHERE profile.reporting_date = CURRENT_DATE()::DATE
),

monthly_consumer AS (
    SELECT profile.*
    FROM {{ monthly_input }} AS profile
    WHERE profile.reporting_date IN ('2026-08-31'::DATE, '2026-09-30'::DATE)
)

SELECT
    (SELECT COUNT(*) FROM current_consumer) AS current_rows,
    (SELECT COUNT(*) FROM monthly_consumer) AS monthly_rows
WHERE (SELECT COUNT(*) FROM current_consumer) <> 1
    OR (SELECT COUNT(*) FROM monthly_consumer) <> 2
    OR (SELECT COUNT_IF(latest_bp_date <> DATEADD(day, -2, CURRENT_DATE())::DATE) FROM current_consumer) <> 0
