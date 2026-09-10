{{ config(materialized='view') }}

-- NICE IND324: https://www.nice.org.uk/indicators/ind324
-- SGLT2 inhibitor order in 6 months for people on the CKD register with type 2 diabetes, or without it and on (or contraindicated to) ACE inhibitor or ARB therapy with eGFR 20 to 44, or eGFR 45 to 59 with ACR 22.6 or more; excludes eGFR below 20.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ckd_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE NOT COALESCE(profile.latest_egfr_value < 20, FALSE)
        AND (
            profile.diabetes_type = 'Type 2'
            OR (
                (COALESCE(profile.latest_ras_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE)
                    OR (profile.is_ace_inhibitor_contraindicated AND profile.is_arb_contraindicated))
                AND (
                    profile.latest_egfr_value BETWEEN 20 AND 44
                    OR (profile.latest_egfr_value BETWEEN 45 AND 59 AND profile.latest_acr_value >= 22.6)
                )
            )
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_acr_value,
        population.latest_egfr_value,
        population.latest_sglt2_order_date AS latest_therapy_order_date,
        CASE WHEN population.latest_sglt2_order_date >= DATEADD(month, -6, CURRENT_DATE()) THEN population.latest_sglt2_order_date END AS latest_record_date,
        COALESCE(population.latest_sglt2_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND324' AS indicator_id,
    'Kidney conditions: CKD and SGLT2 inhibitors' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'CKD with type 2 diabetes, or eGFR 20 to 59 on renin-angiotensin therapy (with ACR 22.6 or more at eGFR 45 to 59)' AS condition_name,
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
