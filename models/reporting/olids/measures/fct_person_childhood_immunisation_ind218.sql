{{ config(materialized='view') }}

-- NICE IND218: https://www.nice.org.uk/indicators/ind218
-- At least one MMR dose between the first and fifth birthdays for children who reached 5 in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        age.age,
        profile.birth_date_approx,
        DATEADD(year, 5, profile.birth_date_approx) AS milestone_date,
        profile.mmr_doses_1_to_5_years AS doses_in_window
    FROM {{ ref('int_childhood_immunisation_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE DATEADD(year, 5, profile.birth_date_approx) BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
        -- NICE excludes children with a contraindication to the vaccine
        AND NOT profile.has_mmr_contraindication
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.birth_date_approx,
        population.milestone_date,
        population.doses_in_window,
        population.doses_in_window >= 1 AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND218' AS indicator_id,
    'Immunisation: MMR (5 years)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Children reaching 5 years in the preceding 12 months' AS condition_name,
    current_practice_code,
    current_practice_name,
    birth_date_approx,
    milestone_date,
    doses_in_window,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
