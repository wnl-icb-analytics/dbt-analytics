{{ config(materialized='view') }}

-- NICE IND195: https://www.nice.org.uk/indicators/ind195
-- Heart failure review, NYHA assessment and medication review in 12 months for people on the heart failure register.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_heart_failure
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_heart_failure_review_date AS latest_review_date,
        population.latest_nyha_date AS latest_nyha_date,
        population.latest_medication_review_date AS latest_medication_review_date,
        CASE WHEN population.latest_heart_failure_review_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_heart_failure_review_date END AS latest_record_date,
        COALESCE(population.latest_heart_failure_review_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_nyha_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_medication_review_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND195' AS indicator_id,
    'Heart failure: annual review' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Heart failure' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_review_date,
    latest_nyha_date,
    latest_medication_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
