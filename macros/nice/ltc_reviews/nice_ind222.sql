{% macro nice_ind222(reference='current') %}
{#-
    Calculate NICE IND222 for newly diagnosed cancer members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND222 detail columns, one person per reporting_date.
-#}
-- NICE IND222: https://www.nice.org.uk/indicators/ind222
WITH indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        register.latest_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('CAN', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE register.latest_diagnosis_date::DATE > DATEADD(month, -15, population.reporting_date)
        AND register.latest_diagnosis_date::DATE <= DATEADD(month, -3, population.reporting_date)
),
support AS (
    SELECT population.person_id, population.reporting_date,
        MAX(event.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_cancer_support_all') }} AS event
        ON population.person_id = event.person_id
        AND event.clinical_effective_date::DATE BETWEEN population.diagnosis_date
            AND LEAST(DATEADD(month, 3, population.diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
)
SELECT population.person_id, 'IND222' AS indicator_id,
    'Cancer: review within 3 months' AS indicator_name,
    population.reporting_date, DATEADD(month, -15, population.reporting_date) AS measurement_period_start,
    population.age, 'Cancer diagnosed 15 to 3 months ago'::VARCHAR(43) AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    population.diagnosis_date, support.latest_record_date,
    TRUE AS is_in_denominator, support.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(support.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM indicator_population AS population
LEFT JOIN support
    ON population.person_id = support.person_id
    AND population.reporting_date = support.reporting_date
{% endmacro %}
