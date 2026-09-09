{{ config(materialized='view') }}

-- NICE IND219: https://www.nice.org.uk/indicators/ind219
-- Shingles vaccination between the 70th and 75th birthdays for people who reached 75 in the preceding 12 months; excludes immunosuppressed people.
WITH indicator_population AS (
    SELECT
        age.person_id,
        age.age,
        age.birth_date_approx::DATE AS birth_date_approx,
        DATEADD(year, 75, age.birth_date_approx)::DATE AS seventy_fifth_birthday
    FROM {{ ref('dim_person_age') }} AS age
    LEFT JOIN {{ ref('int_adult_imms_current_population') }} AS adult
        ON age.person_id = adult.person_id
    WHERE DATEADD(year, 75, age.birth_date_approx) BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
        -- NICE excludes immunocompromised people
        AND NOT COALESCE(adult.is_immunosuppressed, FALSE)
),

doses AS (
    SELECT
        population.person_id,
        MIN(shingles.vaccination_date::DATE) AS first_dose_70_to_75_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('fct_shingles_vaccination_status') }} AS shingles
        ON population.person_id = shingles.person_id
    WHERE shingles.vaccination_status = 'VACCINATION_ADMINISTERED'
        AND shingles.vaccination_date::DATE
            BETWEEN DATEADD(year, 70, population.birth_date_approx) AND population.seventy_fifth_birthday
    GROUP BY population.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.seventy_fifth_birthday,
        doses.first_dose_70_to_75_date,
        doses.first_dose_70_to_75_date AS latest_record_date,
        doses.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN doses
        ON population.person_id = doses.person_id
)

SELECT
    person_id,
    'IND219' AS indicator_id,
    'Immunisation: shingles' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Reached 75 in the preceding 12 months' AS condition_name,
    current_practice_code,
    current_practice_name,
    seventy_fifth_birthday,
    first_dose_70_to_75_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
