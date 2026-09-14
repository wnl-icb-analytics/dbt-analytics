{{ config(materialized='view') }}

-- NICE IND120: https://www.nice.org.uk/indicators/ind120
-- NICE corrected the renal process to eGFR creatinine measurement in February 2026.
-- Count performed foot and ACR tests anywhere in the period, even if a later
-- foot record is declined or the ACR test has no numeric result.
WITH egfr AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_egfr_date
    FROM {{ ref('int_egfr_test_all') }}
    WHERE clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY person_id
),

foot AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_foot_date
    FROM {{ ref('int_foot_examination_all') }}
    WHERE clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
        AND (both_feet_checked
            OR (left_foot_checked AND (right_foot_absent OR right_foot_amputated))
            OR (right_foot_checked AND (left_foot_absent OR left_foot_amputated)))
    GROUP BY person_id
),

acr AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_acr_date
    FROM {{ ref('int_urine_acr_all') }}
    WHERE is_acr_ratio
        AND clinical_effective_date::DATE
            BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY person_id
),

assessed AS (
    SELECT
        processes.person_id,
        age.age,
        active.current_practice_code,
        active.current_practice_name,
        processes.care_processes_completed
            - IFF(COALESCE(processes.creatinine_completed_in_last_12m, FALSE), 1, 0)
            - IFF(COALESCE(processes.foot_check_completed_in_last_12m, FALSE), 1, 0)
            - IFF(COALESCE(processes.acr_completed_in_last_12m, FALSE), 1, 0)
            + IFF(foot.latest_foot_date IS NOT NULL, 1, 0)
            + IFF(acr.latest_acr_date IS NOT NULL, 1, 0)
            + IFF(egfr.latest_egfr_date IS NOT NULL, 1, 0) AS care_processes_completed_count,
        GREATEST_IGNORE_NULLS(
            processes.latest_bmi_date, processes.latest_bp_date, processes.latest_hba1c_date, processes.latest_cholesterol_date,
            processes.latest_smoking_date, foot.latest_foot_date, acr.latest_acr_date,
            egfr.latest_egfr_date
        )::DATE AS latest_process_date
    FROM {{ ref('fct_person_diabetes_8_care_processes') }} AS processes
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON processes.person_id = active.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON processes.person_id = age.person_id
    LEFT JOIN egfr
        ON processes.person_id = egfr.person_id
    LEFT JOIN foot
        ON processes.person_id = foot.person_id
    LEFT JOIN acr
        ON processes.person_id = acr.person_id
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
