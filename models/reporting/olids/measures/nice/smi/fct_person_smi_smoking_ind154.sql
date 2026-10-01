{{ config(materialized='view') }}

-- NICE IND154: https://www.nice.org.uk/indicators/ind154
-- Smoking status recorded in 12 months for people with an active SMI diagnosis; a never-smoker reaching 26 by the end of the financial year is covered by a never-smoked record made after their 25th birthday and after their earliest SMI diagnosis.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_active_smi_diagnosis
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_smoking_status AS latest_smoking_status,
        population.latest_smoking_status_date AS latest_smoking_status_date,
        CASE WHEN population.latest_smoking_status_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_smoking_status_date END AS latest_record_date,
        COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
        OR COALESCE(
            DATEADD(year, 26, population.birth_date_approx)
                <= DATE_FROM_PARTS(YEAR(CURRENT_DATE()) + IFF(MONTH(CURRENT_DATE()) >= 4, 1, 0), 3, 31)
            AND population.latest_smoking_status = 'Never Smoked'
            AND population.latest_never_smoked_date > DATEADD(year, 25, population.birth_date_approx)
            AND population.latest_never_smoked_date > population.earliest_smi_diagnosis_date,
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND154' AS indicator_id,
    'Smoking: smoking status of people with bipolar, schizophrenia and other psychoses' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission)' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_smoking_status,
    latest_smoking_status_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
