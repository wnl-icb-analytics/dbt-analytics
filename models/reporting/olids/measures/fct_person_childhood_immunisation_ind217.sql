{{ config(materialized='view') }}

-- NICE IND217: https://www.nice.org.uk/indicators/ind217
-- A 4-in-1 preschool booster and at least two MMR doses between the first and fifth birthdays for children who reached 5 in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        age.age,
        profile.birth_date_approx,
        DATEADD(year, 5, profile.birth_date_approx) AS milestone_date,
        profile.mmr_doses_1_to_5_years AS doses_in_window,
        profile.has_dtap_booster_1_to_5_years
    FROM {{ ref('int_childhood_immunisation_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE DATEADD(year, 5, profile.birth_date_approx) > DATEADD(month, -12, CURRENT_DATE())
        AND DATEADD(year, 5, profile.birth_date_approx) <= CURRENT_DATE()
        -- NICE excludes children with a contraindication to the vaccine
        AND NOT profile.has_mmr_contraindication
        AND NOT profile.has_dtap_contraindication
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
        population.doses_in_window >= 2 AND population.has_dtap_booster_1_to_5_years AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND217' AS indicator_id,
    'Immunisation: DTaP/IPV and MMR (5 years)' AS indicator_name,
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
