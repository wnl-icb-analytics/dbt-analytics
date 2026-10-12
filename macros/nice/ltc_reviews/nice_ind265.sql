{% macro nice_ind265(reference='current') %}
{#-
    Calculate NICE IND265 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND265 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND265: https://www.nice.org.uk/indicators/ind265
-- Learning disability health check and health action plan both recorded in 12 months for people on the learning disability register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        review.latest_ld_health_check_date,
        review.latest_ld_health_action_plan_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_learning_disability
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_ld_health_check_date AS latest_review_date,
        population.latest_ld_health_action_plan_date AS latest_health_action_plan_date,
        CASE
            WHEN population.latest_ld_health_check_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_ld_health_check_date
        END AS latest_record_date,
        -- The latest plan must follow the latest check, even if an earlier pair qualified.
        COALESCE(population.latest_ld_health_check_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_ld_health_action_plan_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_ld_health_action_plan_date >= population.latest_ld_health_check_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND265' AS indicator_id,
    'Learning disabilities: health checks and action plans' AS indicator_name,
    'The percentage of patients on the learning disability register who received a learning disability health check and had a completed health action plan in the preceding 12 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Learning disability' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_review_date,
    latest_health_action_plan_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
