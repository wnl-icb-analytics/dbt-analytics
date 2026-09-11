{{ config(materialized='view') }}

-- NICE IND81: https://www.nice.org.uk/indicators/ind81
-- Foot examination with a risk classification in 15 months on the diabetes register, allowing an absent or amputated foot in the examination but excluding anyone with a foot amputation.
WITH indicator_population AS (
    SELECT
        diabetes.person_id,
        age.age
    FROM {{ ref('fct_person_diabetes_register') }} AS diabetes
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON diabetes.person_id = age.person_id
    LEFT JOIN {{ ref('int_foot_examination_latest') }} AS foot
        ON diabetes.person_id = foot.person_id
    WHERE diabetes.is_on_register
        -- NICE excludes people who have had a foot amputated
        AND NOT COALESCE(foot.left_foot_amputated OR foot.right_foot_amputated, FALSE)
),

-- Latest examination date in the period with both feet checked (or one with the other absent) and a risk classification
qualifying_examination AS (
    SELECT
        obs.person_id,
        MAX(obs.clinical_effective_date::DATE) AS latest_record_date,
        MAX_BY(obs.diabetes_foot_risk_category, obs.clinical_effective_date) AS latest_foot_risk_category
    FROM {{ ref('int_foot_examination_all') }} AS obs
    INNER JOIN indicator_population AS population
        ON obs.person_id = population.person_id
    WHERE obs.clinical_effective_date::DATE BETWEEN DATEADD(month, -15, CURRENT_DATE()) AND CURRENT_DATE()
        AND NOT obs.is_declined AND NOT obs.is_unsuitable
        AND obs.has_risk_classification
        AND (
            obs.both_feet_checked
            OR (obs.left_foot_checked AND obs.right_foot_absent)
            OR (obs.right_foot_checked AND obs.left_foot_absent)
        )
    GROUP BY obs.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        exam.latest_record_date,
        exam.latest_foot_risk_category,
        exam.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN qualifying_examination AS exam
        ON population.person_id = exam.person_id
)

SELECT
    person_id,
    'IND81' AS indicator_id,
    'Diabetes: annual foot exam and risk classification' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -15, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes without a foot amputation' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_foot_risk_category,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
