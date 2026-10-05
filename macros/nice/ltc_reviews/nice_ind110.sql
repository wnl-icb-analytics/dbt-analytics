{% macro nice_ind110(reference='current') %}
{#-
    Calculate NICE IND110 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND110 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND110: https://www.nice.org.uk/indicators/ind110
-- Rheumatoid arthritis annual review recorded in 15 months for people on the rheumatoid arthritis register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        review.latest_rheumatoid_arthritis_review_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_rheumatoid_arthritis
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_rheumatoid_arthritis_review_date AS latest_review_date,
        CASE
            WHEN population.latest_rheumatoid_arthritis_review_date >= DATEADD(month, -15, population.reporting_date)
                THEN population.latest_rheumatoid_arthritis_review_date
        END AS latest_record_date,
        COALESCE(population.latest_rheumatoid_arthritis_review_date >= DATEADD(month, -15, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND110' AS indicator_id,
    'Rheumatoid arthritis: annual review' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Rheumatoid arthritis' AS condition_name,
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
