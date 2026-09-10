{{ config(materialized='view') }}

-- NICE IND240: hypertension blood pressure control for people aged 80 years and over.
WITH indicator_population AS (
    SELECT
        register.person_id,
        age.age
    FROM {{ ref('fct_person_hypertension_register') }} AS register
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON register.person_id = age.person_id
    WHERE register.is_on_register = TRUE
        AND age.age >= 80
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
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
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 145
            ELSE 150
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
    'IND240' AS indicator_id,
    'Hypertension: blood pressure (80 years and over)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    current_practice_code,
    current_practice_name,
    'Hypertension' AS condition_name,
    latest_bp_date,
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
