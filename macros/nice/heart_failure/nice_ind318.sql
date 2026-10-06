{% macro nice_ind318(reference='current') %}
{#-
    Calculate NICE IND318 for eligible heart failure members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND318 detail columns, one person per reporting_date.
-#}
-- NICE IND318: https://www.nice.org.uk/indicators/ind318
WITH candidates AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        register.earliest_unresolved_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_register('HF', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id AND register.reporting_date = population.reporting_date
    WHERE register.earliest_unresolved_diagnosis_date::DATE BETWEEN '2026-04-01'::DATE AND population.reporting_date
),
category_evidence AS (
    SELECT candidates.person_id, candidates.reporting_date,
        MAX(evidence.clinical_effective_date::DATE) AS latest_record_date,
        COALESCE(MAX(evidence.is_reduced_ef), FALSE) AS has_reduced_ef_category,
        COALESCE(MAX(evidence.is_mildly_reduced_ef), FALSE) AS has_mildly_reduced_ef_category,
        COALESCE(MAX(evidence.is_preserved_ef), FALSE) AS has_preserved_ef_category
    FROM candidates
    LEFT JOIN {{ ref('int_heart_failure_ef_category_all') }} AS evidence
        ON candidates.person_id = evidence.person_id
        AND {{ ltc_register_known_by('evidence.clinical_effective_date', 'evidence.date_recorded', 'candidates.reporting_date') }}
    GROUP BY candidates.person_id, candidates.reporting_date
)
SELECT
    candidates.person_id,
    'IND318' AS indicator_id,
    'Heart failure: ejection fraction category' AS indicator_name,
    'The percentage of patients with a diagnosis of heart failure on or after 1 April 2026 who have a recorded ejection fraction category (reduced, mildly reduced, or preserved).' AS indicator_description,
    candidates.reporting_date,
    '2026-04-01'::DATE AS measurement_period_start,
    candidates.age,
    'Heart failure diagnosed since 1 April 2026' AS denominator_description,
    {{ nice_practice_columns('candidates', reference) }},
    candidates.diagnosis_date,
    evidence.has_reduced_ef_category,
    evidence.has_mildly_reduced_ef_category,
    evidence.has_preserved_ef_category,
    evidence.latest_record_date,
    TRUE AS is_in_denominator,
    evidence.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(evidence.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM candidates
INNER JOIN category_evidence AS evidence
    ON candidates.person_id = evidence.person_id AND candidates.reporting_date = evidence.reporting_date
{% endmacro %}
