{% macro nice_ind134(reference='current') %}
{#-
    Calculate NICE IND134 at each reference date using its reviewed rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND134: https://www.nice.org.uk/indicators/ind134
-- ACE inhibitor or ARB order in 6 months for diabetes with proteinuria or microalbuminuria; excludes contraindications to both classes.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
),

proteinuria AS (
    SELECT
        person_id,
        MIN(clinical_effective_date::DATE) AS first_proteinuria_date
    FROM {{ ref('int_proteinuria_all') }}
    WHERE source_cluster_id IN ('PRT_COD', 'MAL_COD')
    GROUP BY person_id
),

persisting_contraindications AS (
    SELECT
        person_id,
        MIN(IFF(drug_class = 'ACE_INHIBITOR', clinical_effective_date::DATE, NULL)) AS first_ace_date,
        MIN(IFF(drug_class = 'ARB', clinical_effective_date::DATE, NULL)) AS first_arb_date
    FROM {{ ref('int_ras_contraindication_all') }}
    WHERE is_persisting
    GROUP BY person_id
),

expiring_ace_days AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_ras_contraindication_all') }}
    WHERE NOT is_persisting
        AND drug_class = 'ACE_INHIBITOR'
    GROUP BY person_id, clinical_effective_date::DATE
),

expiring_arb_days AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_ras_contraindication_all') }}
    WHERE NOT is_persisting
        AND drug_class = 'ARB'
    GROUP BY person_id, clinical_effective_date::DATE
),

contraindications AS (
    SELECT
        population.person_id,
        population.reporting_date,
        COALESCE(persisting.first_ace_date <= population.reporting_date, FALSE)
            OR COALESCE(ace.event_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AS has_ace_contraindication,
        COALESCE(persisting.first_arb_date <= population.reporting_date, FALSE)
            OR COALESCE(arb.event_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AS has_arb_contraindication
    FROM indicator_population AS population
    LEFT JOIN persisting_contraindications AS persisting
        ON population.person_id = persisting.person_id
    -- Daily ASOF selection avoids copying lifelong evidence across all later month-ends.
    ASOF JOIN expiring_ace_days AS ace
        MATCH_CONDITION (population.reporting_date >= ace.event_date)
        ON population.person_id = ace.person_id
    ASOF JOIN expiring_arb_days AS arb
        MATCH_CONDITION (population.reporting_date >= arb.event_date)
        ON population.person_id = arb.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        therapy.latest_ras_order_date AS latest_therapy_order_date,
        therapy.latest_ras_class AS latest_therapy_class,
        CASE WHEN therapy.latest_ras_order_date >= DATEADD(month, -6, population.reporting_date)
            THEN therapy.latest_ras_order_date END AS latest_record_date,
        COALESCE(therapy.latest_ras_order_date >= DATEADD(month, -6, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN proteinuria
        ON population.person_id = proteinuria.person_id
        AND proteinuria.first_proteinuria_date <= population.reporting_date
    LEFT JOIN contraindications AS contra
        ON population.person_id = contra.person_id
        AND population.reporting_date = contra.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_therapy_evidence', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
    -- Both classes must be contraindicated to exclude the person.
    WHERE NOT (COALESCE(contra.has_ace_contraindication, FALSE)
        AND COALESCE(contra.has_arb_contraindication, FALSE))
)

SELECT
    person_id,
    'IND134' AS indicator_id,
    'Diabetes: ACEi or ARBs' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Diabetes with proteinuria or microalbuminuria' AS condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_therapy_order_date,
    latest_therapy_class,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_therapy_order_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
