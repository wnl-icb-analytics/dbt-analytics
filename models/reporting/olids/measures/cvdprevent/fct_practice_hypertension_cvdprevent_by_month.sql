{{ config(materialized='table', tags=['monthly-full'], cluster_by=['reporting_date', 'practice_code']) }}

-- Hypertension prevalence and treatment to target by practice and month-end, on the CVDPREVENT
-- definitions (CVDP001HYP, CVDP007HYP): adults on the register; latest BP in 12 months at or
-- below 140/90 (150/90 aged 80+), or 135/85 (145/85 aged 80+) at home or ambulatory.

WITH population AS (
    SELECT
        person_id,
        reporting_date,
        age,
        practice_code,
        practice_name
    FROM {{ ref('int_nice_reference_population_by_month') }}
    WHERE age >= 18
),

adults AS (
    SELECT
        reporting_date,
        practice_code,
        MAX(practice_name) AS practice_name,
        COUNT(*) AS adults_registered
    FROM population
    GROUP BY reporting_date, practice_code
),

register_bp AS (
    SELECT
        population.reporting_date,
        population.practice_code,
        COALESCE(
            bp.latest_bp_date::DATE
                BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date,
            FALSE
        ) AS has_bp_in_last_12m,
        bp.is_valid_bp,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        CASE
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' AND population.age >= 80 THEN 145
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 135
            WHEN population.age >= 80 THEN 150
            ELSE 140
        END AS systolic_threshold,
        IFF(bp.applied_measurement_context = 'HBPM_ABPM', 85, 90) AS diastolic_threshold
    FROM ({{ nice_register('HTN', 'by_month') }}) AS register
    INNER JOIN population
        ON register.person_id = population.person_id
        AND register.reporting_date = population.reporting_date
    LEFT JOIN {{ ref('int_nice_blood_pressure_latest_by_month') }} AS bp
        ON register.person_id = bp.person_id
        AND register.reporting_date = bp.reporting_date
),

register_counts AS (
    SELECT
        reporting_date,
        practice_code,
        COUNT(*) AS hypertension_register,
        COUNT_IF(has_bp_in_last_12m) AS bp_recorded_last_12m,
        COUNT_IF(
            has_bp_in_last_12m
            AND is_valid_bp
            AND latest_systolic_value <= systolic_threshold
            AND latest_diastolic_value <= diastolic_threshold
        ) AS treated_to_target
    FROM register_bp
    GROUP BY reporting_date, practice_code
)

SELECT
    adults.reporting_date,
    adults.practice_code,
    adults.practice_name,
    adults.adults_registered,
    COALESCE(register_counts.hypertension_register, 0) AS hypertension_register,
    COALESCE(register_counts.bp_recorded_last_12m, 0) AS bp_recorded_last_12m,
    COALESCE(register_counts.treated_to_target, 0) AS treated_to_target
FROM adults
LEFT JOIN register_counts
    ON adults.reporting_date = register_counts.reporting_date
    AND adults.practice_code = register_counts.practice_code
