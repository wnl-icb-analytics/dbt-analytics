{{ config(materialized='view') }}

-- NICE IND137: https://www.nice.org.uk/indicators/ind137
-- Retinal screening recorded in 12 months on the diabetes register.
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
    FROM {{ ref('int_retinal_screening_all') }} AS obs
    INNER JOIN indicator_population AS population
        ON obs.person_id = population.person_id
    WHERE obs.clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
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
    'IND137' AS indicator_id,
    'Diabetes: annual retinal screening' AS indicator_name,
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
