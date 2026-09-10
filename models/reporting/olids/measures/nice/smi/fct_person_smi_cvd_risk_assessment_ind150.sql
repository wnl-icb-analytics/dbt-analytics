{{ config(materialized='view') }}

-- NICE IND150: https://www.nice.org.uk/indicators/ind150
-- CVD risk assessment in 12 months for people aged 25 to 84 with an active SMI diagnosis, excluding existing CVD, CKD, familial hypercholesterolaemia and type 1 diabetes.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age,
        cvd_risk.latest_risk_assessment_date
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    LEFT JOIN {{ ref('int_cvd_risk_profile') }} AS cvd_risk
        ON profile.person_id = cvd_risk.person_id
    WHERE profile.has_active_smi_diagnosis AND age.age BETWEEN 25 AND 84
        AND profile.earliest_cvd_diagnosis_date IS NULL
        AND NOT COALESCE(cvd_risk.has_ckd, FALSE)
        AND NOT COALESCE(cvd_risk.has_familial_hypercholesterolaemia, FALSE)
        AND NOT COALESCE(cvd_risk.has_type1_diabetes, FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        CASE WHEN population.latest_risk_assessment_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_risk_assessment_date END AS latest_record_date,
        COALESCE(population.latest_risk_assessment_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND150' AS indicator_id,
    'Cardiovascular disease prevention: cardiovascular risk assessment for people with bipolar, schizophrenia or other psychoses' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission), aged 25 to 84, without CVD, CKD, familial hypercholesterolaemia or type 1 diabetes' AS condition_name,
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
