{{ config(materialized='view') }}

-- NICE IND197: https://www.nice.org.uk/indicators/ind197
-- Brief intervention within 3 months of any qualifying positive screen (FAST 3 or more, AUDIT-C 5 or more) for people with a first hypertension diagnosis in the preceding 12 months and any recorded positive screen; excludes alcohol-related disorders.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.earliest_hypertension_date BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
        AND profile.latest_positive_alcohol_screen_date IS NOT NULL
        AND NOT profile.has_alcohol_disorder
),

qualifying_interventions AS (
    SELECT person_id, MAX(latest_intervention_date) AS latest_record_date
    FROM {{ ref('int_nice_alcohol_screen_intervention') }}
    GROUP BY person_id
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
        population.earliest_hypertension_date AS new_diagnosis_date,
        interventions.latest_record_date,
        interventions.latest_record_date IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN qualifying_interventions AS interventions
        ON population.person_id = interventions.person_id
)

SELECT
    person_id,
    'IND197' AS indicator_id,
    'Alcohol use: brief intervention for people with hypertension' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Newly diagnosed hypertension with a positive alcohol screen' AS condition_name,
    current_practice_code,
    current_practice_name,
    new_diagnosis_date,
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
