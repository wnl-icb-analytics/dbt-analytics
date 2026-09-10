{{ config(materialized='view') }}

-- NICE IND159: https://www.nice.org.uk/indicators/ind159
-- Blood glucose or HbA1c recorded in 12 months for adults with an active SMI diagnosis, excluding diabetes diagnosed more than 12 months ago.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_active_smi_diagnosis AND age.age >= 18
        AND NOT COALESCE(profile.earliest_diabetes_diagnosis_date < DATEADD(month, -12, CURRENT_DATE()), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_glucose_or_hba1c_date AS latest_glucose_or_hba1c_date,
        CASE WHEN population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_glucose_or_hba1c_date END AS latest_record_date,
        COALESCE(population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND159' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: annual blood glucose or HbA1c' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission), aged 18 and over, without diabetes diagnosed more than 12 months ago' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_glucose_or_hba1c_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
