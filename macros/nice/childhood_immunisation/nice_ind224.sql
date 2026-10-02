{% macro nice_ind224(reference='current') %}
{#-
    Calculate NICE IND224 from milestone cohorts and dated vaccine evidence.
    Args: reference is current or by_month.
    Returns: one eligible child per reporting_date with the original detail columns.
-#}
-- NICE IND224: https://www.nice.org.uk/indicators/ind224
-- Two rotavirus doses before 24 weeks of age for babies who reached 24 weeks in the preceding 12 months.
WITH profile AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        rotavirus_doses_by_24_weeks,
        has_rotavirus_contraindication
    FROM {{ nice_ref('int_childhood_immunisation_profile', reference) }} AS profile
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        profile.birth_date_approx,
        DATEADD(week, 24, profile.birth_date_approx) AS milestone_date,
        profile.rotavirus_doses_by_24_weeks AS doses_in_window,
        profile.rotavirus_doses_by_24_weeks >= 2 AS is_in_numerator
    FROM profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE DATEADD(week, 24, profile.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(week, 24, profile.birth_date_approx) <= population.reporting_date
        -- NICE excludes rotavirus contraindication or history of vaccine allergy.
        AND NOT profile.has_rotavirus_contraindication
)

SELECT
    assessed.person_id,
    'IND224' AS indicator_id,
    'Immunisation: rotavirus (24 weeks)' AS indicator_name,
    assessed.reporting_date,
    DATEADD(month, -12, assessed.reporting_date) AS measurement_period_start,
    assessed.age,
    'Babies reaching 24 weeks in the preceding 12 months' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    assessed.birth_date_approx,
    assessed.milestone_date,
    assessed.doses_in_window,
    TRUE AS is_in_denominator,
    assessed.is_in_numerator,
    CASE
        WHEN assessed.is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
