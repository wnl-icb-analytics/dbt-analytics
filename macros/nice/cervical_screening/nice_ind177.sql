{% macro nice_ind177(reference='current') %}
{#-
    Calculate NICE IND177 at each reference date with today's rules.
    Args: reference is current or by_month.
    Returns: indicator detail, one eligible person per reporting_date.
-#}
-- NICE IND177: https://www.nice.org.uk/indicators/ind177
-- Cervical screening recorded in 5.5 years for women aged 50 to 64; excludes people without a cervix, non-response to three invitations in the screening interval and pregnancy.
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
        AND population.age BETWEEN 50 AND 64
        AND screening.first_cervix_removal_date IS NULL
        -- Completed screening takes precedence over non-response in the same interval.
        AND (
            screening.latest_completed_date >= DATEADD(month, -66, population.reporting_date)
            OR screening.latest_non_response_date IS NULL
            OR screening.latest_non_response_date::DATE <= DATEADD(month, -66, population.reporting_date)
        )
        AND NOT screening.is_currently_pregnant
)

SELECT
    person_id,
    'IND177' AS indicator_id,
    'Screening: cervical screening (50 to 64 years)' AS indicator_name,
    reporting_date AS reporting_date,
    DATEADD(month, -66, reporting_date) AS measurement_period_start,
    age,
    'Women aged 50 to 64' AS condition_name,
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
