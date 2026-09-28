{{ config(materialized='view') }}

-- NICE IND169: https://www.nice.org.uk/indicators/ind169
-- Anticoagulant medication review in 12 months for people over 18 on the AF register with an oral anticoagulant order in the preceding 6 months.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_atrial_fibrillation_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age > 18
        AND COALESCE(profile.latest_anticoagulant_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_chadsvasc_score,
        population.latest_chadsvasc_date,
        population.latest_chads2_score,
        population.latest_anticoagulant_order_date,
        population.latest_anticoagulant_type,
        population.latest_doac_order_date,
        population.latest_vka_order_date,
        population.is_doac_ineligible,
        population.has_doac_exception,
        population.latest_anticoagulant_review_date,
        COALESCE(population.latest_anticoagulant_review_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND169' AS indicator_id,
    'Atrial fibrillation: review of anticoagulation' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Atrial fibrillation on anticoagulation' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_chadsvasc_score,
    latest_chadsvasc_date,
    latest_chads2_score,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_doac_order_date,
    latest_vka_order_date,
    is_doac_ineligible,
    has_doac_exception,
    latest_anticoagulant_review_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
