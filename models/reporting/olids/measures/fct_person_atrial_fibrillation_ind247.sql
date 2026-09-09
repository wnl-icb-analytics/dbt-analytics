{{ config(materialized='view') }}

-- NICE IND247: https://www.nice.org.uk/indicators/ind247
-- DOAC order in 6 months, or a VKA order where a DOAC is ineligible (valvular AF, antiphospholipid syndrome),
-- contraindicated, declined or not indicated, for people on the AF register with a latest CHA2DS2-VASc of 2 or more.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_atrial_fibrillation_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.latest_chadsvasc_score >= 2
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
        COALESCE(population.latest_doac_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE) AS is_doac_in_period,
        COALESCE(population.latest_vka_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE) AS is_vka_in_period,
        -- Sequential criteria: DOAC unless ineligible (valvular AF, antiphospholipid syndrome),
        -- then VKA where a DOAC is ineligible, contraindicated, declined or not indicated
        CASE
            WHEN population.is_doac_ineligible THEN is_vka_in_period
            ELSE is_doac_in_period OR (is_vka_in_period AND population.has_doac_exception)
        END AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND247' AS indicator_id,
    'Atrial fibrillation: DOACs and Vitamin K antagonists' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Atrial fibrillation with CHA2DS2-VASc 2 or more' AS condition_name,
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
        WHEN is_doac_ineligible AND is_doac_in_period THEN 'DOAC_WHERE_VKA_INDICATED'
        WHEN is_vka_in_period THEN 'VKA_WITHOUT_DOAC_EXCEPTION'
        WHEN latest_anticoagulant_order_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
