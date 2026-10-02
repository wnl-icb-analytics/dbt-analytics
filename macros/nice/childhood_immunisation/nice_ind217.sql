{% macro nice_ind217(reference='current') %}
{#-
    Calculate NICE IND217 from milestone cohorts and dated vaccine evidence.
    Args: reference is current or by_month.
    Returns: one eligible child per reporting_date with the original detail columns.
-#}
-- NICE IND217: https://www.nice.org.uk/indicators/ind217
-- A 4-in-1 preschool booster and at least two MMR doses between the first and fifth birthdays for children who reached 5 in the preceding 12 months.
WITH profile AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        mmr_doses_1_to_5_years,
        has_dtap_booster_1_to_5_years,
        has_mmr_contraindication,
        has_dtap_contraindication
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
        DATEADD(year, 5, profile.birth_date_approx) AS milestone_date,
        profile.mmr_doses_1_to_5_years AS doses_in_window,
        profile.mmr_doses_1_to_5_years >= 2 AND profile.has_dtap_booster_1_to_5_years AS is_in_numerator
    FROM profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE DATEADD(year, 5, profile.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(year, 5, profile.birth_date_approx) <= population.reporting_date
        -- Current contraindication proxy for NICE confirmed-anaphylaxis exclusions.
        AND NOT profile.has_mmr_contraindication
        AND NOT profile.has_dtap_contraindication
)

SELECT
    assessed.person_id,
    'IND217' AS indicator_id,
    'Immunisation: DTaP/IPV and MMR (5 years)' AS indicator_name,
    assessed.reporting_date,
    DATEADD(month, -12, assessed.reporting_date) AS measurement_period_start,
    assessed.age,
    'Children reaching 5 years in the preceding 12 months' AS condition_name,
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
