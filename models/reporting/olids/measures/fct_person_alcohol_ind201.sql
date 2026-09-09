{{ config(materialized='view') }}

-- NICE IND201: https://www.nice.org.uk/indicators/ind201
-- FAST, AUDIT-C or AUDIT screen in the preceding 2 years for people with a listed LTC; excludes alcohol-related disorders.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE (
            profile.has_chd OR profile.has_atrial_fibrillation OR profile.has_heart_failure
            OR profile.has_stroke_tia OR profile.has_diabetes OR profile.has_dementia
        )
        AND NOT profile.has_alcohol_disorder
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_alcohol_screen_date,
        population.latest_alcohol_screen_tool,
        population.latest_alcohol_screen_score,
        population.latest_positive_alcohol_screen_date,
        population.latest_intervention_after_positive_screen_date,
        CASE WHEN COALESCE(population.latest_alcohol_screen_date >= DATEADD(month, -24, CURRENT_DATE()), FALSE)
            THEN population.latest_alcohol_screen_date END AS latest_record_date,
        COALESCE(population.latest_alcohol_screen_date >= DATEADD(month, -24, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND201' AS indicator_id,
    'Alcohol use: risk assessment for people with a long-term condition' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -24, CURRENT_DATE()) AS measurement_period_start,
    age,
    'CHD, atrial fibrillation, heart failure, stroke/TIA, diabetes or dementia' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_alcohol_screen_date,
    latest_alcohol_screen_tool,
    latest_alcohol_screen_score,
    latest_positive_alcohol_screen_date,
    latest_intervention_after_positive_screen_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
