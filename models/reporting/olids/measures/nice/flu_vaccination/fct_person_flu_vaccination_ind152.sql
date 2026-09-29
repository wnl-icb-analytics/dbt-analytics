{{ config(materialized='view') }}

-- NICE IND152: https://www.nice.org.uk/indicators/ind152
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the CHD, stroke/TIA, diabetes or COPD register.
WITH register AS (
    SELECT person_id FROM {{ ref('fct_person_chd_register') }} WHERE is_on_register
    UNION
    SELECT person_id FROM {{ ref('fct_person_stroke_tia_register') }} WHERE is_on_register
    UNION
    SELECT person_id FROM {{ ref('fct_person_diabetes_register') }} WHERE is_on_register
    UNION
    SELECT person_id FROM {{ ref('fct_person_copd_register') }} WHERE is_on_register
),

indicator_population AS (
    SELECT
        register.person_id,
        age.age
    FROM register
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON register.person_id = age.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        season.latest_vaccination_date,
        season.is_laiv,
        season.latest_vaccination_date AS latest_record_date,
        season.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('int_nice_flu_season_vaccination') }} AS season
        ON population.person_id = season.person_id
)

SELECT
    person_id,
    'IND152' AS indicator_id,
    'Immunisation: flu vaccine for people with long-term conditions' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATE_FROM_PARTS({{ nice_flu_season_year() }}, 8, 1) AS measurement_period_start,
    age,
    'CHD, stroke/TIA, diabetes or COPD' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_vaccination_date,
    is_laiv,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
