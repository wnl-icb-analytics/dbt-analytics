{{ config(materialized='view') }}

-- NICE IND143: https://www.nice.org.uk/indicators/ind143
-- Mental health care plan recorded in 12 months for people with an active SMI diagnosis.
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
        CASE WHEN population.latest_smi_care_plan_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_smi_care_plan_date END AS latest_record_date,
        COALESCE(population.latest_smi_care_plan_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND143' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: care planning' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission)' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
