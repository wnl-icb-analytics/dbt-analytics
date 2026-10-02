{% macro nice_ind226(reference='current') %}
{#-
    Calculate NICE IND226 from milestone cohorts and dated vaccine evidence.
    Args: reference is current or by_month.
    Returns: one eligible child per reporting_date with the original detail columns.
-#}
-- NICE IND226: https://www.nice.org.uk/indicators/ind226
-- Two primary MenB doses before 12 months and one booster from 12 months, all before 18 months of age, for children who reached 18 months in the preceding 12 months.
WITH profile AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        menb_doses_by_18_months,
        menb_primary_doses_by_12_months,
        menb_booster_doses_12_to_18_months,
        has_menb_contraindication
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
        DATEADD(month, 18, profile.birth_date_approx) AS milestone_date,
        profile.menb_doses_by_18_months AS doses_in_window,
        profile.menb_primary_doses_by_12_months AS primary_doses_by_12_months,
        profile.menb_booster_doses_12_to_18_months AS booster_doses_12_to_18_months,
        profile.menb_primary_doses_by_12_months >= 2
            AND profile.menb_booster_doses_12_to_18_months >= 1 AS is_in_numerator
    FROM profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE DATEADD(month, 18, profile.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(month, 18, profile.birth_date_approx) <= population.reporting_date
        -- NICE excludes children with a contraindication to the vaccine
        AND NOT profile.has_menb_contraindication
)

SELECT
    assessed.person_id,
    'IND226' AS indicator_id,
    'Immunisation: meningitis B (18 months)' AS indicator_name,
    assessed.reporting_date,
    DATEADD(month, -12, assessed.reporting_date) AS measurement_period_start,
    assessed.age,
    'Children reaching 18 months in the preceding 12 months' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    assessed.birth_date_approx,
    assessed.milestone_date,
    assessed.doses_in_window,
    assessed.primary_doses_by_12_months,
    assessed.booster_doses_12_to_18_months,
    TRUE AS is_in_denominator,
    assessed.is_in_numerator,
    CASE
        WHEN assessed.is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
