{{ config(materialized='view') }}

-- NICE IND276: https://www.nice.org.uk/indicators/ind276
-- Lipid-lowering therapy in the last 6 months for people on the diabetes register and the CHD, stroke/TIA or PAD register without haemorrhagic stroke history.
WITH indicator_population AS (
    SELECT
        cvd.person_id,
        age.age
    FROM {{ ref('int_cvd_secondary_prevention_population') }} AS cvd
    INNER JOIN {{ ref('fct_person_diabetes_register') }} AS diabetes
        ON cvd.person_id = diabetes.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON cvd.person_id = age.person_id
    WHERE diabetes.is_on_register
        AND NOT cvd.has_haemorrhagic_stroke
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
    'IND276' AS indicator_id,
    'Diabetes: lipid-lowering therapies for secondary prevention of CVD' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes with cardiovascular disease' AS condition_name,
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
