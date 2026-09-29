{{ config(materialized='view') }}

-- NICE IND269: https://www.nice.org.uk/indicators/ind269
-- CVD risk assessment recorded in 5 years for people aged 45 to 84; excludes type 1 diabetes, CVD, FH, CKD, current lipid-lowering therapy and a risk score of 20% or more ever.
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
    WHERE age.age BETWEEN 45 AND 84
        AND NOT profile.has_type1_diabetes
        AND NOT profile.has_cvd
        AND NOT profile.has_familial_hypercholesterolaemia
        AND NOT profile.has_ckd
        AND NOT COALESCE(profile.latest_lipid_lowering_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE)
        AND NOT COALESCE(profile.max_risk_score_ever >= 20, FALSE)
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
        -- NICE counts a recorded risk score, so scored QRISK results only
        COALESCE(
            population.latest_risk_score_date >= DATEADD(month, -60, CURRENT_DATE()),
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND269' AS indicator_id,
    'Cardiovascular disease prevention: risk assessment (general population)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -60, CURRENT_DATE()) AS measurement_period_start,
    age,
    'General population aged 45 to 84' AS condition_name,
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
