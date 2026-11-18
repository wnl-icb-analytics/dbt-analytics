{% macro calculate_aki_register(reference='current', reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
{#-
    Calculate NICE IND174 membership for adults with a recorded AKI episode.
    Args: reference selects the population; reference_dates supplies reference_date rows.
    Returns: one register member per person and reference_date, with known diagnosis dates.
-#}
-- NICE IND174: https://www.nice.org.uk/indicators/ind174
WITH reference_dates AS (
    {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
), population AS (
    SELECT population.person_id, dates.reference_date, population.age, population.practice_code
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN reference_dates AS dates ON population.reporting_date = dates.reference_date
    WHERE population.age >= 18
), diagnoses AS (
    SELECT population.person_id, population.reference_date,
        MIN(diagnosis.clinical_effective_date) AS earliest_diagnosis_date,
        MAX(diagnosis.clinical_effective_date) AS latest_diagnosis_date
    FROM population
    INNER JOIN {{ ref('int_aki_diagnoses_all') }} AS diagnosis
        ON population.person_id = diagnosis.person_id
        AND {{ ltc_register_known_by('diagnosis.clinical_effective_date', 'diagnosis.date_recorded', 'population.reference_date') }}
    GROUP BY population.person_id, population.reference_date
)
SELECT population.person_id, population.reference_date, population.age, population.practice_code,
    TRUE AS is_on_register, diagnoses.earliest_diagnosis_date, diagnoses.latest_diagnosis_date
FROM population
INNER JOIN diagnoses ON population.person_id = diagnoses.person_id
    AND population.reference_date = diagnoses.reference_date
{% endmacro %}
