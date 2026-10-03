{% macro nice_hypertension_new_diagnosis_population(reference='current') %}
{#-
    Select the shared new-hypertension cohort for target organ damage tests.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the diagnosis anchor.
-#}
SELECT
    population.person_id,
    population.reporting_date,
    population.age,
    population.practice_code,
    population.practice_name,
    hypertension.earliest_diagnosis_date::DATE AS diagnosis_date
FROM ({{ nice_reference_population(reference) }}) AS population
INNER JOIN ({{ nice_register('HTN', reference) }}) AS hypertension
    ON population.person_id = hypertension.person_id
    AND population.reporting_date = hypertension.reporting_date
WHERE hypertension.earliest_diagnosis_date::DATE
        BETWEEN {{ nice_financial_year_start('population.reporting_date') }} AND population.reporting_date
{% endmacro %}
