{{ config(materialized='view') }}

-- NICE IND120: https://www.nice.org.uk/indicators/ind120
-- All eight care processes (BMI, BP, HbA1c, cholesterol, smoking status, foot examination, ACR, eGFR or creatinine) in 12 months on the diabetes register, from fct_person_diabetes_8_care_processes.
WITH assessed AS (
    SELECT
        processes.person_id,
        age.age,
        active.current_practice_code,
        active.current_practice_name,
        processes.care_processes_completed AS care_processes_completed_count,
        CASE WHEN processes.all_processes_completed THEN GREATEST_IGNORE_NULLS(
            processes.latest_bmi_date, processes.latest_bp_date, processes.latest_hba1c_date, processes.latest_cholesterol_date,
            processes.latest_smoking_date, processes.latest_foot_check_date, processes.latest_acr_date, processes.latest_creatinine_date
        )::DATE END AS latest_record_date,
        COALESCE(processes.all_processes_completed, FALSE) AS is_in_numerator
    FROM {{ ref('fct_person_diabetes_8_care_processes') }} AS processes
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON processes.person_id = active.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON processes.person_id = age.person_id
)

SELECT
    person_id,
    'IND120' AS indicator_id,
    'Diabetes: annual general practice checks' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes' AS condition_name,
    current_practice_code,
    current_practice_name,
    care_processes_completed_count,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
