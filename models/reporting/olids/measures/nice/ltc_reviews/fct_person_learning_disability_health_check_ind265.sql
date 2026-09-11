{{ config(materialized='view') }}

-- NICE IND265: https://www.nice.org.uk/indicators/ind265
-- Learning disability health check and health action plan both recorded in 12 months for people on the learning disability register.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_learning_disability
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_ld_health_check_date AS latest_review_date,
        population.latest_ld_health_action_plan_date AS latest_health_action_plan_date,
        CASE WHEN population.latest_ld_health_check_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_ld_health_check_date END AS latest_record_date,
        COALESCE(population.latest_ld_health_check_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_ld_health_action_plan_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_ld_health_action_plan_date >= population.latest_ld_health_check_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND265' AS indicator_id,
    'Learning disabilities: health checks and action plans' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Learning disability' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_review_date,
    latest_health_action_plan_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
