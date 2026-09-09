{{ config(materialized='view') }}

-- NICE IND208: https://www.nice.org.uk/indicators/ind208
-- Asked about falls in 12 months for people aged 65 and over with moderate or severe coded frailty.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age >= 65
        AND profile.latest_frailty_severity IN ('Moderate', 'Severe')
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.ltc_count,
        population.latest_frailty_severity,
        CASE WHEN COALESCE(population.latest_falls_discussion_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            THEN population.latest_falls_discussion_date END AS latest_record_date,
        COALESCE(population.latest_falls_discussion_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND208' AS indicator_id,
    'Multiple long-term conditions: asking about falls' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Aged 65 and over with moderate or severe frailty' AS condition_name,
    current_practice_code,
    current_practice_name,
    ltc_count,
    latest_frailty_severity,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
