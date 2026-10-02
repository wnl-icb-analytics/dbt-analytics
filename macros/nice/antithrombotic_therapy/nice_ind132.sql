{% macro nice_ind132(reference='current') %}
{#- Calculate IND132 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND132: https://www.nice.org.uk/indicators/ind132
-- Antiplatelet or oral anticoagulant evidence in 12 months on the CHD register.
-- Excludes people contraindicated to all three of salicylates, clopidogrel and oral anticoagulants (persisting at any time or expiring in 12 months), as NICE lists and QOF CHD005 applies.
WITH candidates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_register('CHD', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id
        AND register.reporting_date = population.reporting_date
),

persisting AS (
    SELECT
        person_id,
        drug_class,
        MIN(clinical_effective_date::DATE) AS first_date
    FROM {{ ref('int_antithrombotic_contraindication_all') }}
    WHERE is_persisting
    GROUP BY person_id, drug_class
),

expiring AS (
    SELECT DISTINCT
        person_id,
        drug_class,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_antithrombotic_contraindication_all') }}
    WHERE NOT is_persisting
),

class_candidates AS (
    SELECT
        candidates.person_id,
        candidates.reporting_date,
        classes.value::VARCHAR AS drug_class
    FROM candidates
    CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('SALICYLATE', 'CLOPIDOGREL', 'ORAL_ANTICOAGULANT'))) AS classes
),

selected_expiring AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        candidate.drug_class,
        evidence.event_date
    FROM class_candidates AS candidate
    ASOF JOIN expiring AS evidence
        MATCH_CONDITION (candidate.reporting_date >= evidence.event_date)
        ON candidate.person_id = evidence.person_id
        AND candidate.drug_class = evidence.drug_class
),

contraindicated AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        COUNT_IF(COALESCE(persisting.first_date <= candidate.reporting_date, FALSE)
            OR COALESCE(candidate.event_date >= DATEADD(month, -12, candidate.reporting_date), FALSE)) AS contraindicated_class_count
    FROM selected_expiring AS candidate
    LEFT JOIN persisting
        ON candidate.person_id = persisting.person_id
        AND candidate.drug_class = persisting.drug_class
    GROUP BY candidate.person_id, candidate.reporting_date
),

indicator_population AS (
    SELECT candidates.*
    FROM candidates
    LEFT JOIN contraindicated
        ON candidates.person_id = contraindicated.person_id
        AND candidates.reporting_date = contraindicated.reporting_date
    WHERE NOT COALESCE(contraindicated.contraindicated_class_count >= 3, FALSE)
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
            >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_antiplatelet_in_period,
        COALESCE(GREATEST_IGNORE_NULLS(therapy.latest_anticoagulant_order_date, records.latest_anticoagulant_record_date)
            >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_anticoagulant_in_period
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_therapy_evidence', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
    LEFT JOIN selected_records AS records
        ON population.person_id = records.person_id
        AND population.reporting_date = records.reporting_date
)

SELECT
    person_id,
    'IND132' AS indicator_id,
    'Angina and coronary heart disease: anti-platelet or anticoagulation' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Coronary heart disease' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
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
{% endmacro %}
