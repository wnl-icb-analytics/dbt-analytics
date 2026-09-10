{{ config(materialized='view') }}

-- NICE IND200: https://www.nice.org.uk/indicators/ind200
-- Brief intervention within 3 months of the latest positive screen for people on the SMI register with a positive screen in the preceding 12 months; excludes alcohol-related disorders.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_smi
        AND profile.latest_positive_alcohol_screen_date >= DATEADD(month, -12, CURRENT_DATE())
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
        population.latest_intervention_after_positive_screen_date AS latest_record_date,
        population.latest_intervention_after_positive_screen_date IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND200' AS indicator_id,
    'Alcohol use: brief intervention for people with SMI' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Severe mental illness with a positive alcohol screen' AS condition_name,
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
