{#-
    Calculate NICE IND192 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND192 detail columns, one person per reporting_date.
-#}
{% macro nice_ind192(reference='current') %}
-- NICE IND192: https://www.nice.org.uk/indicators/ind192
-- Echo or specialist assessment within three months either side of heart failure diagnosis for diagnoses three to fifteen months before reporting.
WITH candidates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        register.earliest_unresolved_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_register('HF', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id AND register.reporting_date = population.reporting_date
    WHERE register.earliest_unresolved_diagnosis_date::DATE > DATEADD(month, -15, population.reporting_date)
        AND register.earliest_unresolved_diagnosis_date::DATE <= DATEADD(month, -3, population.reporting_date)
),
confirmation AS (
    SELECT candidates.person_id, candidates.reporting_date, MAX(evidence.clinical_effective_date::DATE) AS latest_record_date
    FROM candidates
    LEFT JOIN {{ ref('int_heart_failure_confirmation_all') }} AS evidence
        ON candidates.person_id = evidence.person_id
        AND evidence.clinical_effective_date::DATE BETWEEN DATEADD(month, -3, candidates.diagnosis_date)
            AND LEAST(DATEADD(month, 3, candidates.diagnosis_date), candidates.reporting_date)
    GROUP BY candidates.person_id, candidates.reporting_date
)
SELECT
    candidates.person_id,
    'IND192' AS indicator_id,
    'Heart failure: confirmation of diagnosis' AS indicator_name,
    candidates.reporting_date,
    DATEADD(month, -15, candidates.reporting_date) AS measurement_period_start,
    candidates.age,
    'New unresolved heart failure diagnosis' AS condition_name,
    {{ nice_practice_columns('candidates', reference) }},
    candidates.diagnosis_date,
    confirmation.latest_record_date,
    TRUE AS is_in_denominator,
    confirmation.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(confirmation.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM candidates
LEFT JOIN confirmation ON candidates.person_id = confirmation.person_id AND candidates.reporting_date = confirmation.reporting_date
{% endmacro %}
