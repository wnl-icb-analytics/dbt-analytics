{{ config(materialized='view') }}

-- NICE IND321: https://www.nice.org.uk/indicators/ind321
-- Cervical screening recorded in 5.5 years for women aged 25 to 64; excludes unsuitable (no cervix).
WITH indicator_population AS (
    SELECT
        demographics.person_id,
        age.age
    FROM {{ ref('dim_person_demographics') }} AS demographics
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON demographics.person_id = age.person_id
    LEFT JOIN {{ ref('fct_cervical_screening_status') }} AS screening
        ON demographics.person_id = screening.person_id
    WHERE demographics.gender = 'Female'
        AND age.age BETWEEN 25 AND 64
        -- No cervix or otherwise unsuitable: latest screening record is an unsuitable code
        AND NOT COALESCE(screening.latest_is_unsuitable, FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        screening.latest_completed_date,
        screening.latest_screening_date,
        screening.programme_status,
        CASE WHEN screening.latest_completed_date >= DATEADD(month, -66, CURRENT_DATE())
            THEN screening.latest_completed_date END AS latest_record_date,
        COALESCE(screening.latest_completed_date >= DATEADD(month, -66, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('fct_cervical_screening_status') }} AS screening
        ON population.person_id = screening.person_id
)

SELECT
    person_id,
    'IND321' AS indicator_id,
    'Screening: cervical (25 to 64 years)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -66, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Women aged 25 to 64' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_completed_date,
    latest_screening_date,
    programme_status,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
