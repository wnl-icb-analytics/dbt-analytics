{{ config(materialized='view') }}

-- NICE IND161: https://www.nice.org.uk/indicators/ind161
-- CVD risk assessment within 3 months either side of a first hypertension or type 2 diabetes diagnosis in the preceding 12 months, aged 25 to 84; excludes CVD, CKD, FH and type 1 diabetes.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        age.age,
        profile.latest_risk_score,
        profile.latest_risk_score_date,
        profile.latest_risk_assessment_date,
        LEAST_IGNORE_NULLS(
            CASE WHEN profile.earliest_hypertension_date >= DATEADD(month, -12, CURRENT_DATE())
                THEN profile.earliest_hypertension_date END,
            CASE WHEN profile.earliest_type2_diabetes_date >= DATEADD(month, -12, CURRENT_DATE())
                THEN profile.earliest_type2_diabetes_date END
        ) AS new_diagnosis_date
    FROM {{ ref('int_cvd_risk_profile') }} AS profile
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age BETWEEN 25 AND 84
        AND (
            profile.earliest_hypertension_date >= DATEADD(month, -12, CURRENT_DATE())
            OR profile.earliest_type2_diabetes_date >= DATEADD(month, -12, CURRENT_DATE())
        )
        AND NOT profile.has_cvd
        AND NOT profile.has_ckd
        AND NOT profile.has_familial_hypercholesterolaemia
        AND NOT profile.has_type1_diabetes
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
        population.new_diagnosis_date,
        EXISTS (
            SELECT 1
            FROM {{ ref('int_cvd_risk_assessment_all') }} AS assessment
            WHERE assessment.person_id = population.person_id
                AND assessment.clinical_effective_date::DATE
                    BETWEEN DATEADD(month, -3, population.new_diagnosis_date)
                    AND DATEADD(month, 3, population.new_diagnosis_date)
        ) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND161' AS indicator_id,
    'Cardiovascular disease prevention: cardiovascular risk assessment for people newly diagnosed with hypertension or T2DM' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Newly diagnosed hypertension or type 2 diabetes' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    new_diagnosis_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
