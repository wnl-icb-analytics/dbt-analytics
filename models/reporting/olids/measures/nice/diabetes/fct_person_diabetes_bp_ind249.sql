{{ config(materialized='view') }}

-- NICE IND249: https://www.nice.org.uk/indicators/ind249
-- Last BP in 12 months below 140/90 clinic or 135/85 home, diabetes register aged 17 to 79 without moderate or severe frailty.
WITH indicator_population AS (
    SELECT
        diabetes.person_id,
        age.age,
        frailty.latest_frailty_severity
    FROM {{ ref('fct_person_diabetes_register') }} AS diabetes
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON diabetes.person_id = age.person_id
    LEFT JOIN {{ ref('fct_person_frailty_register') }} AS frailty
        ON diabetes.person_id = frailty.person_id
    WHERE diabetes.is_on_register
        AND age.age BETWEEN 17 AND 79
        AND COALESCE(frailty.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_frailty_severity,
        bp.latest_bp_date,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        bp.applied_measurement_context,
        COALESCE(
            bp.latest_bp_date::DATE
                BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE(),
            FALSE
        ) AS is_bp_recorded_in_last_12m,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 135
            ELSE 140
        END AS indicator_systolic_threshold,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 85
            ELSE 90
        END AS indicator_diastolic_threshold
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('fct_person_bp_control') }} AS bp
        ON population.person_id = bp.person_id
),

status AS (
    SELECT
        *,
        COALESCE(
            latest_systolic_value < indicator_systolic_threshold
            AND latest_diastolic_value < indicator_diastolic_threshold,
            FALSE
        ) AS is_latest_bp_within_indicator_target
    FROM assessed
)

SELECT
    person_id,
    'IND249' AS indicator_id,
    'Diabetes: blood pressure (without moderate or severe frailty)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes without moderate or severe frailty' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_frailty_severity,
    latest_bp_date,
    CASE WHEN is_bp_recorded_in_last_12m THEN latest_bp_date END AS latest_record_date,
    latest_systolic_value,
    latest_diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    TRUE AS is_in_denominator,
    is_bp_recorded_in_last_12m,
    is_latest_bp_within_indicator_target,
    is_bp_recorded_in_last_12m
        AND is_latest_bp_within_indicator_target AS is_in_numerator,
    CASE
        WHEN NOT is_bp_recorded_in_last_12m THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN is_latest_bp_within_indicator_target THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM status
