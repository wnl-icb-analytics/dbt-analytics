{{ config(materialized='view') }}

-- NICE IND157: https://www.nice.org.uk/indicators/ind157
-- Offer of smoking cessation support recorded in 12 months for current smokers with a listed LTC.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE (
            profile.has_chd OR profile.has_pad OR profile.has_stroke_tia OR profile.has_hypertension
            OR profile.has_diabetes OR profile.has_copd OR profile.has_ckd OR profile.has_asthma
        )
        AND profile.latest_smoking_status = 'Current Smoker'
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_smoking_status,
        population.latest_smoking_status_date,
        population.latest_never_smoked_date,
        population.latest_smoking_intervention_date,
        CASE WHEN COALESCE(population.latest_smoking_intervention_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            THEN population.latest_smoking_intervention_date END AS latest_record_date,
        COALESCE(population.latest_smoking_intervention_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND157' AS indicator_id,
    'Smoking: support and treatment for people with long-term conditions' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Current smoker with a long-term condition' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_smoking_status,
    latest_smoking_status_date,
    latest_never_smoked_date,
    latest_smoking_intervention_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
