{{ config(materialized='view') }}

-- NICE IND120: https://www.nice.org.uk/indicators/ind120
-- All eight care processes (BMI, BP, HbA1c, cholesterol, smoking status, foot examination, ACR, eGFR or creatinine) in 12 months on the diabetes register, from fct_person_diabetes_8_care_processes with eGFR accepted for the renal process.
-- The care process model reads serum creatinine for the renal process; NICE names eGFR, so either counts here
WITH egfr AS (
    SELECT person_id, clinical_effective_date::DATE AS latest_egfr_date
    FROM {{ ref('int_egfr_latest') }}
    WHERE egfr_value IS NOT NULL
),

assessed AS (
    SELECT
        processes.person_id,
        age.age,
        active.current_practice_code,
        active.current_practice_name,
        processes.care_processes_completed
            - IFF(COALESCE(processes.creatinine_completed_in_last_12m, FALSE), 1, 0)
            + IFF(COALESCE(egfr.latest_egfr_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
                  OR COALESCE(processes.creatinine_completed_in_last_12m, FALSE), 1, 0) AS care_processes_completed_count,
        GREATEST_IGNORE_NULLS(
            processes.latest_bmi_date, processes.latest_bp_date, processes.latest_hba1c_date, processes.latest_cholesterol_date,
            processes.latest_smoking_date, processes.latest_foot_check_date, processes.latest_acr_date, processes.latest_creatinine_date,
            egfr.latest_egfr_date
        )::DATE AS latest_process_date
    FROM {{ ref('fct_person_diabetes_8_care_processes') }} AS processes
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON processes.person_id = active.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON processes.person_id = age.person_id
    LEFT JOIN egfr
        ON processes.person_id = egfr.person_id
),

status AS (
    SELECT
        *,
        care_processes_completed_count = 8 AS is_in_numerator,
        CASE WHEN care_processes_completed_count = 8 THEN latest_process_date END AS latest_record_date
    FROM assessed
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
FROM status
