{{ config(materialized='view') }}

-- NICE IND104: https://www.nice.org.uk/indicators/ind104
-- Depression review 10 to 35 days after a new depression diagnosis for adults diagnosed in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.latest_new_depression_diagnosis_date >= DATEADD(month, -12, CURRENT_DATE()) AND age.age >= 18
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_new_depression_diagnosis_date AS diagnosis_date,
        population.first_depression_review_10_to_35_days_date AS latest_review_date,
        population.first_depression_review_10_to_35_days_date AS latest_record_date,
        population.first_depression_review_10_to_35_days_date IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND104' AS indicator_id,
    'Depression and anxiety: review within 10 to 35 days' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'New depression diagnosis in the preceding 12 months (aged 18 and over)' AS condition_name,
    current_practice_code,
    current_practice_name,
    diagnosis_date,
    latest_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
