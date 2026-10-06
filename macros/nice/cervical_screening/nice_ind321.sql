{% macro nice_ind321(reference='current') %}
{#-
    Calculate NICE IND321 at each reference date with today's rules.
    Args: reference is current or by_month.
    Returns: indicator detail, one eligible person per reporting_date.
-#}
-- NICE IND321: https://www.nice.org.uk/indicators/ind321
-- Cervical screening recorded in 5.5 years for women aged 25 to 64; excludes people without a cervix.
WITH assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        screening.latest_completed_date,
        screening.latest_screening_date,
        screening.programme_status,
        CASE WHEN screening.latest_completed_date >= DATEADD(month, -66, population.reporting_date)
            THEN screening.latest_completed_date END AS latest_record_date,
        COALESCE(screening.latest_completed_date >= DATEADD(month, -66, population.reporting_date), FALSE) AS is_in_numerator
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_cervical_screening_evidence', reference) }} AS screening
        ON population.person_id = screening.person_id
        AND population.reporting_date = screening.reporting_date
    WHERE population.gender = 'Female'
        AND population.age BETWEEN 25 AND 64
        AND screening.first_cervix_removal_date IS NULL
)

SELECT
    person_id,
    'IND321' AS indicator_id,
    'Screening: cervical (25 to 64 years)' AS indicator_name,
    'The percentage of women eligible for cervical screening and aged 25 to 64 years at end of the period reported whose notes record that an adequate cervical screening test has been performed in the previous 5.5 years.' AS indicator_description,
    reporting_date AS reporting_date,
    DATEADD(month, -66, reporting_date) AS measurement_period_start,
    age,
    'Women aged 25 to 64' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_completed_date,
    latest_screening_date,
    programme_status,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
