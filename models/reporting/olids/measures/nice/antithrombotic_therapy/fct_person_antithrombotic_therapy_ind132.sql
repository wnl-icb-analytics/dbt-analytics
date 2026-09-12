{{ config(materialized='view') }}

-- NICE IND132: https://www.nice.org.uk/indicators/ind132
-- Antiplatelet or oral anticoagulant evidence in 12 months on the CHD register.
-- Excludes people contraindicated to all three of salicylates, clopidogrel and oral anticoagulants (persisting at any time or expiring in 12 months), as NICE lists and QOF CHD005 applies.
WITH
-- Classes contraindicated: persisting at any time or expiring in the preceding 12 months (QOF reading)
contraindicated AS (
    SELECT
        person_id,
        COUNT(DISTINCT drug_class) AS contraindicated_class_count
    FROM {{ ref('int_antithrombotic_contraindication_all') }}
    WHERE (is_persisting OR clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE()))
        AND drug_class IN ('SALICYLATE', 'CLOPIDOGREL', 'ORAL_ANTICOAGULANT')
    GROUP BY person_id
),

indicator_population AS (
    SELECT
        register.person_id,
        age.age
    FROM {{ ref('fct_person_chd_register') }} AS register
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON register.person_id = age.person_id
    LEFT JOIN contraindicated
        ON register.person_id = contraindicated.person_id
    WHERE register.is_on_register
        AND NOT COALESCE(contraindicated.contraindicated_class_count >= 3, FALSE)
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
        therapy.latest_antiplatelet_record_date,
        therapy.latest_anticoagulant_record_date,
        COALESCE(GREATEST_IGNORE_NULLS(therapy.latest_antiplatelet_order_date, therapy.latest_antiplatelet_record_date)
            >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_antiplatelet_in_period,
        COALESCE(GREATEST_IGNORE_NULLS(therapy.latest_anticoagulant_order_date, therapy.latest_anticoagulant_record_date)
            >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_anticoagulant_in_period
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('int_antithrombotic_therapy_latest') }} AS therapy
        ON population.person_id = therapy.person_id
)

SELECT
    person_id,
    'IND132' AS indicator_id,
    'Angina and coronary heart disease: anti-platelet or anticoagulation' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Coronary heart disease' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_antiplatelet_order_date,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_antiplatelet_record_date,
    latest_anticoagulant_record_date,
    is_antiplatelet_in_period,
    is_anticoagulant_in_period,
    TRUE AS is_in_denominator,
    is_antiplatelet_in_period OR is_anticoagulant_in_period AS is_in_numerator,
    CASE
        WHEN is_antiplatelet_in_period OR is_anticoagulant_in_period THEN 'ACHIEVED'
        WHEN latest_antiplatelet_order_date IS NOT NULL
            OR latest_antiplatelet_record_date IS NOT NULL
            OR latest_anticoagulant_order_date IS NOT NULL
            OR latest_anticoagulant_record_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
