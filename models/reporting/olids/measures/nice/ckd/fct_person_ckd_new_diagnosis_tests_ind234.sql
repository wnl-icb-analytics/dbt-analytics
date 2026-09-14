{{ config(materialized='view') }}

-- NICE IND234: https://www.nice.org.uk/indicators/ind234
-- eGFR and urine ACR both recorded within 90 days before or after diagnosis for people diagnosed with CKD stage 3 to 5 in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ckd_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.ckd_diagnosis_date BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.ckd_diagnosis_date AS diagnosis_date,
        population.latest_egfr_value,
        population.latest_acr_value,
        CASE WHEN population.has_egfr_within_90_days_of_diagnosis AND population.has_acr_within_90_days_of_diagnosis THEN GREATEST(population.egfr_within_90_days_of_diagnosis_date, population.acr_within_90_days_of_diagnosis_date) END AS latest_record_date,
        population.has_egfr_within_90_days_of_diagnosis AND population.has_acr_within_90_days_of_diagnosis AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND234' AS indicator_id,
    'Kidney conditions: CKD - eGFR and ACR' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'CKD stage 3 to 5 diagnosed in the preceding 12 months' AS condition_name,
    current_practice_code,
    current_practice_name,
    diagnosis_date,
    latest_egfr_value,
    latest_acr_value,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
