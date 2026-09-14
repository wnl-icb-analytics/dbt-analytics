{{ config(materialized='view') }}

-- NICE IND130: https://www.nice.org.uk/indicators/ind130
-- ACE inhibitor or ARB order in 6 months for people on the CKD and hypertension registers with proteinuria; excludes people contraindicated to both classes.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ckd_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_hypertension AND profile.has_proteinuria
        AND NOT (profile.is_ace_inhibitor_contraindicated AND profile.is_arb_contraindicated)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_acr_value,
        population.latest_egfr_value,
        population.latest_ras_order_date AS latest_therapy_order_date,
        CASE WHEN population.latest_ras_order_date >= DATEADD(month, -6, CURRENT_DATE()) THEN population.latest_ras_order_date END AS latest_record_date,
        COALESCE(population.latest_ras_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND130' AS indicator_id,
    'Kidney conditions: CKD and renin-angiotensin system antagonists' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'CKD with hypertension and proteinuria' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_acr_value,
    latest_egfr_value,
    latest_therapy_order_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_therapy_order_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD'
    END AS indicator_status
FROM assessed
