{{ config(materialized='view') }}

-- NICE IND248: https://www.nice.org.uk/indicators/ind248
-- All six physical health checks in 12 months for people with an SMI diagnosis, before personalised care adjustments including remission.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.earliest_smi_diagnosis_date IS NOT NULL
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_blood_pressure_date AS latest_blood_pressure_date,
        population.latest_bmi_date AS latest_bmi_date,
        population.latest_alcohol_record_date AS latest_alcohol_record_date,
        population.latest_lipid_date AS latest_lipid_date,
        population.latest_glucose_or_hba1c_date AS latest_glucose_or_hba1c_date,
        population.latest_smoking_status_date AS latest_smoking_status_date,
        IFF(population.latest_blood_pressure_date >= DATEADD(month, -12, CURRENT_DATE()), 1, 0) + IFF(population.latest_bmi_date >= DATEADD(month, -12, CURRENT_DATE()), 1, 0) + IFF(population.latest_alcohol_record_date >= DATEADD(month, -12, CURRENT_DATE()), 1, 0) + IFF(population.latest_lipid_date >= DATEADD(month, -12, CURRENT_DATE()), 1, 0) + IFF(population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, CURRENT_DATE()), 1, 0) + IFF(population.latest_smoking_status_date >= DATEADD(month, -12, CURRENT_DATE()), 1, 0) AS checks_met_count,
        CASE WHEN population.latest_blood_pressure_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_bmi_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_alcohol_record_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_lipid_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, CURRENT_DATE())
            AND population.latest_smoking_status_date >= DATEADD(month, -12, CURRENT_DATE())
            THEN GREATEST(population.latest_blood_pressure_date, population.latest_bmi_date, population.latest_alcohol_record_date, population.latest_lipid_date, population.latest_glucose_or_hba1c_date, population.latest_smoking_status_date) END AS latest_record_date,
        COALESCE(population.latest_blood_pressure_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_bmi_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_alcohol_record_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_lipid_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            AND COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND248' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: 6 physical health checks' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses), including remission before personalised care adjustments' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_blood_pressure_date,
    latest_bmi_date,
    latest_alcohol_record_date,
    latest_lipid_date,
    latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    checks_met_count,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
