{{ config(materialized='view') }}

-- NICE IND275: https://www.nice.org.uk/indicators/ind275
-- Lipid-lowering therapy in 6 months for people with diabetes aged 40 and over, no CVD, no moderate or severe frailty; excludes type 2 diabetes with a CVD risk score below 10% recorded in the preceding 3 years.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        age.age
    FROM {{ ref('int_cvd_risk_profile') }} AS profile
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age >= 40
        AND profile.has_diabetes
        AND NOT profile.has_cvd
        AND COALESCE(profile.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
        AND NOT COALESCE(profile.has_type2_diabetes AND profile.min_risk_score_36m < 10, FALSE)
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
    'IND275' AS indicator_id,
    'Diabetes: lipid-lowering therapies for primary prevention of CVD (40 years and over)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes aged 40 and over without CVD' AS condition_name,
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
