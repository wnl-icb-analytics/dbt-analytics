{% macro nice_ind210(reference='current') %}
{#-
    Calculate NICE IND210 for new registrants at each reference date.
    Args: reference is current or by_month.
    Returns: the IND210 detail columns, one person per reporting_date.
-#}
-- NICE IND210: https://www.nice.org.uk/indicators/ind210
WITH indicator_population AS (
    SELECT population.*
    FROM ({{ nice_reference_population(reference, include_registration_start=true) }}) AS population
    WHERE population.age >= 16
        AND population.registration_start_date > DATEADD(month, -15, population.reporting_date)
        AND population.registration_start_date <= DATEADD(month, -3, population.reporting_date)
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('int_hiv_diagnoses_all') }} AS diagnosis
            WHERE diagnosis.person_id = population.person_id
                AND diagnosis.clinical_effective_date::DATE < population.registration_start_date
                AND (diagnosis.date_recorded IS NULL
                    OR diagnosis.date_recorded::DATE < population.registration_start_date)
        )
),
tests AS (
    SELECT population.person_id, population.reporting_date,
        MAX(test.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_hiv_tests_all') }} AS test
        ON population.person_id = test.person_id
        AND test.clinical_effective_date::DATE BETWEEN population.registration_start_date
            AND LEAST(DATEADD(month, 3, population.registration_start_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
)
SELECT population.person_id, 'IND210' AS indicator_id,
    'HIV: testing at registration' AS indicator_name,
    population.reporting_date, DATEADD(month, -15, population.reporting_date) AS measurement_period_start,
    population.age, 'New registrants aged 16 or over without known HIV' AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    population.registration_start_date, test.latest_record_date,
    TRUE AS is_in_denominator, test.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(test.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM indicator_population AS population
LEFT JOIN tests AS test
    ON population.person_id = test.person_id
    AND population.reporting_date = test.reporting_date
{% endmacro %}
