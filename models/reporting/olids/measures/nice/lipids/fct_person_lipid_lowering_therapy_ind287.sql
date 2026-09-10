{{ config(materialized='view') }}

-- NICE IND287: https://www.nice.org.uk/indicators/ind287
-- Lipid-lowering therapy in 6 months for people aged 25 to 84 with hypertension or type 2 diabetes first diagnosed in the preceding 12 months and a CVD risk score of 10% or more in that period; excludes CVD, CKD, FH and type 1 diabetes.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        age.age
    FROM {{ ref('int_cvd_risk_profile') }} AS profile
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age BETWEEN 25 AND 84
        AND (
            profile.earliest_hypertension_date >= DATEADD(month, -12, CURRENT_DATE())
            OR profile.earliest_type2_diabetes_date >= DATEADD(month, -12, CURRENT_DATE())
        )
        AND profile.max_risk_score_12m >= 10
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
        therapy.latest_order_date AS latest_lipid_lowering_order_date,
        therapy.latest_lipid_lowering_class,
        therapy.latest_product_name AS latest_lipid_lowering_product,
        therapy.is_latest_order_statin AS is_latest_lipid_lowering_statin,
        COALESCE(
            therapy.latest_order_date >= DATEADD(month, -6, CURRENT_DATE()),
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('int_lipid_lowering_therapy_latest') }} AS therapy
        ON population.person_id = therapy.person_id
)

SELECT
    person_id,
    'IND287' AS indicator_id,
    'Cardiovascular disease prevention: lipid lowering therapy for people newly diagnosed with hypertension or T2DM' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Newly diagnosed hypertension or type 2 diabetes with CVD risk score 10% or more' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_lipid_lowering_order_date,
    latest_lipid_lowering_class,
    latest_lipid_lowering_product,
    is_latest_lipid_lowering_statin,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_lipid_lowering_order_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
