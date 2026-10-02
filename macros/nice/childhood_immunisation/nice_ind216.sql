{% macro nice_ind216(reference='current') %}
{#-
    Calculate NICE IND216 from milestone cohorts and dated vaccine evidence.
    Args: reference is current or by_month.
    Returns: one eligible child per reporting_date with the original detail columns.
-#}
-- NICE IND216: https://www.nice.org.uk/indicators/ind216
-- At least one MMR dose between 12 and 18 months of age for children who reached 18 months in the preceding 12 months.
WITH profile AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        mmr_doses_12_to_18_months,
        has_mmr_contraindication
    FROM {{ nice_ref('int_nice_childhood_immunisation_profile', reference) }} AS profile
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        profile.birth_date_approx,
        DATEADD(month, 18, profile.birth_date_approx) AS milestone_date,
        profile.mmr_doses_12_to_18_months AS doses_in_window,
        profile.mmr_doses_12_to_18_months >= 1 AS is_in_numerator
    FROM profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE DATEADD(month, 18, profile.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(month, 18, profile.birth_date_approx) <= population.reporting_date
        -- Current contraindication proxy for NICE confirmed-anaphylaxis exclusions.
        AND NOT profile.has_mmr_contraindication
)

SELECT
    assessed.person_id,
    'IND216' AS indicator_id,
    'Immunisation: MMR (18 months)' AS indicator_name,
    assessed.reporting_date,
    DATEADD(month, -12, assessed.reporting_date) AS measurement_period_start,
    assessed.age,
    'Children reaching 18 months in the preceding 12 months' AS condition_name,
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
