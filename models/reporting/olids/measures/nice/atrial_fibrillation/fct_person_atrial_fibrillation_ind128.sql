{{ config(materialized='view') }}

-- NICE IND128: https://www.nice.org.uk/indicators/ind128
-- Oral anticoagulant order in 6 months for people on the AF register with a latest CHA2DS2-VASc of 2 or more, or no CHA2DS2-VASc and a latest CHADS2 of 2 or more; excludes an anticoagulant allergy or adverse reaction ever, or anticoagulation contraindicated in 12 months.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_atrial_fibrillation_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE (
            profile.latest_chadsvasc_score >= 2
            OR (profile.latest_chadsvasc_date IS NULL AND profile.latest_chads2_score >= 2)
        )
        -- NICE exclusions: persisting contraindication anywhere on the record (allergy or adverse
        -- reaction), or an expiring contraindication recorded in the preceding 12 months
        AND NOT profile.has_anticoagulant_adverse_reaction
        AND NOT profile.has_anticoagulant_persisting_contraindication
        AND NOT COALESCE(profile.latest_anticoagulant_contraindicated_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_chadsvasc_score,
        population.latest_chadsvasc_date,
        population.latest_chads2_score,
        population.latest_anticoagulant_order_date,
        population.latest_anticoagulant_type,
        population.latest_doac_order_date,
        population.latest_vka_order_date,
        population.is_doac_ineligible,
        population.has_doac_exception,
        population.latest_anticoagulant_review_date,
        COALESCE(population.latest_anticoagulant_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND128' AS indicator_id,
    'Atrial fibrillation: current treatment with anticoagulation' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Atrial fibrillation with stroke risk score 2 or more' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_chadsvasc_score,
    latest_chadsvasc_date,
    latest_chads2_score,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_doac_order_date,
    latest_vka_order_date,
    is_doac_ineligible,
    has_doac_exception,
    latest_anticoagulant_review_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_anticoagulant_order_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
