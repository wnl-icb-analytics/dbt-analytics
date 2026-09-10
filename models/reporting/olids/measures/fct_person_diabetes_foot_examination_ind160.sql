{{ config(materialized='view') }}

-- NICE IND160: https://www.nice.org.uk/indicators/ind160
-- Foot examination of both feet in 12 months on the diabetes register, allowing an absent or amputated foot; declined or unsuitable does not count.
WITH indicator_population AS (
    SELECT
        diabetes.person_id,
        age.age
    FROM {{ ref('fct_person_diabetes_register') }} AS diabetes
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON diabetes.person_id = age.person_id
    WHERE diabetes.is_on_register
),

-- Latest qualifying record in the period
latest_record AS (
    SELECT
        obs.person_id,
        MAX(obs.clinical_effective_date::DATE) AS latest_record_date
    FROM {{ ref('int_foot_examination_all') }} AS obs
    INNER JOIN indicator_population AS population
        ON obs.person_id = population.person_id
    WHERE obs.clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
        AND NOT (obs.is_unsuitable OR obs.is_declined)
        AND (
            obs.both_feet_checked
            OR (obs.left_foot_checked AND (obs.right_foot_absent OR obs.right_foot_amputated))
            OR (obs.right_foot_checked AND (obs.left_foot_absent OR obs.left_foot_amputated))
        )
    GROUP BY obs.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        record.latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN latest_record AS record
        ON population.person_id = record.person_id
)

SELECT
    person_id,
    'IND160' AS indicator_id,
    'Diabetes: annual examination of foot sensation' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_record_date,
    TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    CASE
        WHEN latest_record_date IS NOT NULL THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
