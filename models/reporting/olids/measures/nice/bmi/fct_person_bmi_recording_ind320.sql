{{ config(materialized='view') }}

-- NICE IND320: https://www.nice.org.uk/indicators/ind320
-- BMI recorded in 12 months for people with CHD, stroke/TIA, diabetes, non-diabetic hyperglycaemia, hypertension, PAD, heart failure, COPD, dyslipidaemia, learning disability, obstructive sleep apnoea or SMI.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    -- BMI values are modelled for adults only, so the denominator is 18 and over
    WHERE age.age >= 18
        AND (
            profile.has_chd OR profile.has_stroke_tia OR profile.has_diabetes OR profile.has_ndh
            OR profile.has_hypertension OR profile.has_pad OR profile.has_heart_failure OR profile.has_copd
            OR profile.has_dyslipidaemia OR profile.has_learning_disability
            OR profile.has_obstructive_sleep_apnoea OR profile.has_smi
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_bmi_date,
        CASE WHEN COALESCE(population.latest_bmi_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            THEN population.latest_bmi_date END AS latest_record_date,
        COALESCE(population.latest_bmi_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND320' AS indicator_id,
    'Weight management: BMI recording (long term conditions)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Long-term condition on the NICE BMI recording list' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_bmi_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
