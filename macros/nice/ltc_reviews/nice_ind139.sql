{% macro nice_ind139(reference='current') %}
{#-
    Calculate NICE IND139 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND139 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND139: https://www.nice.org.uk/indicators/ind139
-- Thyroid function test recorded in 12 months for people on the hypothyroidism register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        review.latest_thyroid_function_test_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_hypothyroidism
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_thyroid_function_test_date AS latest_review_date,
        CASE
            WHEN population.latest_thyroid_function_test_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_thyroid_function_test_date
        END AS latest_record_date,
        COALESCE(population.latest_thyroid_function_test_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND139' AS indicator_id,
    'Hypothyroidism: annual thyroid function test' AS indicator_name,
    'The percentage of patients with hypothyroidism, on the register, with thyroid function tests recorded in the preceding 12 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Hypothyroidism' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
