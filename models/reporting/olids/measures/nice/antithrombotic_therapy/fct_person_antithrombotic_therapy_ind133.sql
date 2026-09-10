{{ config(materialized='view') }}

-- NICE IND133: https://www.nice.org.uk/indicators/ind133
-- Antiplatelet or oral anticoagulant order in 12 months on the stroke/TIA register. People with haemorrhagic
-- stroke history stay in only through a TIA diagnosis.
WITH tia_history AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_stroke_tia_diagnoses_all') }}
    WHERE is_tia_diagnosis_code
),

indicator_population AS (
    SELECT
        register.person_id,
        age.age
    FROM {{ ref('fct_person_stroke_tia_register') }} AS register
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON register.person_id = age.person_id
    LEFT JOIN {{ ref('int_haemorrhagic_stroke_history') }} AS haemorrhagic
        ON register.person_id = haemorrhagic.person_id
    LEFT JOIN tia_history AS tia
        ON register.person_id = tia.person_id
    -- NICE denominator: stroke shown to be non-haemorrhagic, or a history of TIA
    WHERE register.is_on_register
        AND (haemorrhagic.person_id IS NULL OR tia.person_id IS NOT NULL)
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        therapy.latest_antiplatelet_order_date,
        therapy.latest_anticoagulant_order_date,
        therapy.latest_anticoagulant_type,
        COALESCE(therapy.latest_antiplatelet_order_date
            >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_antiplatelet_in_period,
        COALESCE(therapy.latest_anticoagulant_order_date
            >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_anticoagulant_in_period
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('int_antithrombotic_therapy_latest') }} AS therapy
        ON population.person_id = therapy.person_id
)

SELECT
    person_id,
    'IND133' AS indicator_id,
    'Stroke and ischaemic attack: anti-platelet or anticoagulation' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Non-haemorrhagic stroke or TIA' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_antiplatelet_order_date,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    is_antiplatelet_in_period,
    is_anticoagulant_in_period,
    TRUE AS is_in_denominator,
    is_antiplatelet_in_period OR is_anticoagulant_in_period AS is_in_numerator,
    CASE
        WHEN is_antiplatelet_in_period OR is_anticoagulant_in_period THEN 'ACHIEVED'
        WHEN latest_antiplatelet_order_date IS NOT NULL
            OR latest_anticoagulant_order_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
