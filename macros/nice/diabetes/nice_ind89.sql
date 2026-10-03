{% macro nice_ind89(reference='current') %}
{#-
    Calculate NICE IND89 for diabetes register members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND89 detail columns, one person per reporting_date.
-#}
-- NICE IND89: https://www.nice.org.uk/indicators/ind89
WITH indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
),
reviews AS (
    SELECT population.person_id, population.reporting_date,
        MAX(event.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_diabetes_dietary_reviews_all') }} AS event
        ON population.person_id = event.person_id
        AND event.clinical_effective_date::DATE > DATEADD(month, -15, population.reporting_date)
        AND event.clinical_effective_date::DATE <= population.reporting_date
    GROUP BY population.person_id, population.reporting_date
)
SELECT population.person_id, 'IND89' AS indicator_id,
    'Diabetes: annual dietary review' AS indicator_name,
    population.reporting_date, DATEADD(month, -15, population.reporting_date) AS measurement_period_start,
    population.age, 'Diabetes register' AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    review.latest_record_date,
    TRUE AS is_in_denominator, review.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(review.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM indicator_population AS population
LEFT JOIN reviews AS review
    ON population.person_id = review.person_id
    AND population.reporting_date = review.reporting_date
{% endmacro %}
