{{ config(materialized='view') }}

-- NICE IND88: https://www.nice.org.uk/indicators/ind88
-- Referral to a structured education programme within 9 months of joining the diabetes register, for people diagnosed in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        diabetes.person_id,
        age.age,
        diabetes.earliest_diagnosis_date::DATE AS diagnosis_date
    FROM {{ ref('fct_person_diabetes_register') }} AS diabetes
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON diabetes.person_id = age.person_id
    WHERE diabetes.is_on_register
        AND diabetes.earliest_diagnosis_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
),

first_referral AS (
    SELECT
        population.person_id,
        MIN(education.clinical_effective_date::DATE) AS first_referral_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_diabetes_structured_education_all') }} AS education
        ON population.person_id = education.person_id
        AND education.record_type = 'REFERRED'
        AND education.clinical_effective_date::DATE BETWEEN population.diagnosis_date
            AND DATEADD(month, 9, population.diagnosis_date)
    GROUP BY population.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.diagnosis_date,
        referral.first_referral_date AS latest_record_date,
        referral.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN first_referral AS referral
        ON population.person_id = referral.person_id
)

SELECT
    person_id,
    'IND88' AS indicator_id,
    'Diabetes: referral for structured education' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes diagnosed in the preceding 12 months' AS condition_name,
    current_practice_code,
    current_practice_name,
    diagnosis_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
