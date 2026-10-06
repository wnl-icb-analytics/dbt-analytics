{% macro nice_ind94(reference='current') %}
{#- Calculate IND94 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND94: https://www.nice.org.uk/indicators/ind94
-- Antiplatelet order or recorded OTC salicylate use in 15 months on the PAD register, excluding people with an oral anticoagulant order in the same period.
WITH candidates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_register('PAD', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id
        AND register.reporting_date = population.reporting_date
),

indicator_population AS (
    SELECT candidates.*
    FROM candidates
),

antiplatelet_records AS (
    SELECT DISTINCT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_antithrombotic_treatment_all') }}
    WHERE source_cluster_id IN ('OSAL_COD', 'CLO_COD')
),

anticoagulant_records AS (
    SELECT DISTINCT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_antithrombotic_treatment_all') }}
    WHERE source_cluster_id = 'ORANTICOAG_COD'
),

selected_records AS (
    SELECT
        population.person_id,
        population.reporting_date,
        antiplatelet.event_date AS latest_antiplatelet_record_date,
        anticoagulant.event_date AS latest_anticoagulant_record_date
    FROM indicator_population AS population
    ASOF JOIN antiplatelet_records AS antiplatelet
        MATCH_CONDITION (population.reporting_date >= antiplatelet.event_date)
        ON population.person_id = antiplatelet.person_id
    ASOF JOIN anticoagulant_records AS anticoagulant
        MATCH_CONDITION (population.reporting_date >= anticoagulant.event_date)
        ON population.person_id = anticoagulant.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        therapy.latest_antiplatelet_order_date,
        therapy.latest_anticoagulant_order_date,
        therapy.latest_anticoagulant_type,
        records.latest_antiplatelet_record_date,
        records.latest_anticoagulant_record_date,
        COALESCE(GREATEST_IGNORE_NULLS(therapy.latest_antiplatelet_order_date, records.latest_antiplatelet_record_date)
            >= DATEADD(month, -15, population.reporting_date), FALSE) AS is_antiplatelet_in_period,
        COALESCE(therapy.latest_anticoagulant_order_date
            >= DATEADD(month, -15, population.reporting_date), FALSE) AS is_anticoagulant_in_period
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_therapy_evidence', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
    LEFT JOIN selected_records AS records
        ON population.person_id = records.person_id
        AND population.reporting_date = records.reporting_date
    -- NICE excludes people already prescribed an anticoagulant
    WHERE NOT COALESCE(therapy.latest_anticoagulant_order_date
        >= DATEADD(month, -15, population.reporting_date), FALSE)
)

SELECT
    person_id,
    'IND94' AS indicator_id,
    'Peripheral arterial disease: antiplatelets' AS indicator_name,
    'The percentage of patients with peripheral arterial disease with a record in the preceding 15 months that aspirin or an alternative antiplatelet is being taken.' AS indicator_description,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Peripheral arterial disease' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_antiplatelet_order_date,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_antiplatelet_record_date,
    latest_anticoagulant_record_date,
    is_antiplatelet_in_period,
    is_anticoagulant_in_period,
    TRUE AS is_in_denominator,
    is_antiplatelet_in_period AS is_in_numerator,
    CASE
        WHEN is_antiplatelet_in_period THEN 'ACHIEVED'
        WHEN latest_antiplatelet_order_date IS NOT NULL
            OR latest_antiplatelet_record_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
{% endmacro %}
