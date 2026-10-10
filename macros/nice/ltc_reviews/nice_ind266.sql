{% macro nice_ind266(reference='current') %}
{#-
    Calculate NICE IND266 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND266 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND266: https://www.nice.org.uk/indicators/ind266
-- Learning disability health check and health action plan in 12 months plus a recorded ethnicity for people on the learning disability register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        review.latest_ld_health_check_date,
        review.latest_ld_health_action_plan_date,
        COALESCE(demographics.ethnicity_category NOT IN ('Unknown'), FALSE) AS has_ethnicity_recorded
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    -- C12 uses the demographic interval at the evaluation date in both modes.
    LEFT JOIN {{ ref('dim_person_demographics_historical') }} AS demographics
        ON population.person_id = demographics.person_id
        AND demographics.effective_start_date <= population.reporting_date
        AND (demographics.effective_end_date IS NULL
            OR demographics.effective_end_date > population.reporting_date)
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
        population.has_ethnicity_recorded AS has_ethnicity_recorded,
        CASE
            WHEN population.latest_ld_health_check_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_ld_health_check_date
        END AS latest_record_date,
        -- The latest plan must follow the latest check, even if an earlier pair qualified.
        COALESCE(population.latest_ld_health_check_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_ld_health_action_plan_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_ld_health_action_plan_date >= population.latest_ld_health_check_date, FALSE)
            AND population.has_ethnicity_recorded AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND266' AS indicator_id,
    'Learning disabilities: health checks, action plans and ethnicity' AS indicator_name,
    'The percentage of patients on the learning disability register, who: received a learning disability health check and had a completed health action plan in the preceding 12 months and have a recording of ethnicity.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Learning disability' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_review_date,
    latest_health_action_plan_date,
    has_ethnicity_recorded,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
