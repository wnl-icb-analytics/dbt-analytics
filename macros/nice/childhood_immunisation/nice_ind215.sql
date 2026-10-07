{% macro nice_ind215(reference='current') %}
{#-
    Calculate NICE IND215 using milestone cohorts and dose/contraindication state.
    Args: reference is current or by_month.
    Returns: the IND215 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND215: https://www.nice.org.uk/indicators/ind215
-- Three DTP-containing doses before 8 months of age for babies who reached 8 months in the preceding 12 months.
WITH profile AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        dtap_doses_by_8_months,
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
        DATEADD(month, 8, profile.birth_date_approx) AS milestone_date,
        profile.dtap_doses_by_8_months AS doses_in_window,
        profile.dtap_doses_by_8_months >= 3 AS is_in_numerator
    FROM profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE DATEADD(month, 8, profile.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(month, 8, profile.birth_date_approx) <= population.reporting_date
        -- Current contraindication proxy for NICE confirmed-anaphylaxis exclusions.
        AND NOT profile.has_dtap_contraindication
)

SELECT
    assessed.person_id,
    'IND215' AS indicator_id,
    'Immunisation: DTaP (8 months)' AS indicator_name,
    'The percentage of babies who reached 8 months old in the preceding 12 months, who have received at least 3 doses of a diphtheria, tetanus and pertussis containing vaccine before the age of 8 months.' AS indicator_description,
    assessed.reporting_date,
    DATEADD(month, -12, assessed.reporting_date) AS measurement_period_start,
    assessed.age,
    'Babies reaching 8 months in the preceding 12 months' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    assessed.birth_date_approx,
    assessed.milestone_date,
    assessed.doses_in_window,
    TRUE AS is_in_denominator,
    assessed.is_in_numerator,
    IFF(assessed.is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
