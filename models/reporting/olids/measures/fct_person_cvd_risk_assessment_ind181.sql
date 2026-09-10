{{ config(materialized='view') }}

-- NICE IND181: https://www.nice.org.uk/indicators/ind181
-- CVD risk assessment recorded in 3 years for people aged 25 to 84 with type 2 diabetes, no moderate or severe frailty and no statin order in 6 months; excludes CVD, FH and CKD.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        age.age,
        profile.latest_risk_score,
        profile.latest_risk_score_date,
        profile.latest_risk_assessment_date
    FROM {{ ref('int_cvd_risk_profile') }} AS profile
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age BETWEEN 25 AND 84
        AND profile.has_type2_diabetes
        AND COALESCE(profile.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
        AND NOT COALESCE(profile.latest_statin_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE)
        AND NOT profile.has_cvd
        AND NOT profile.has_familial_hypercholesterolaemia
        AND NOT profile.has_ckd
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_risk_score,
        population.latest_risk_score_date,
        population.latest_risk_assessment_date,
        COALESCE(
            population.latest_risk_assessment_date >= DATEADD(month, -36, CURRENT_DATE()),
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND181' AS indicator_id,
    'Diabetes: CVD risk assessment' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -36, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Type 2 diabetes not on a statin' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
