{% macro nice_ind214(reference='current') %}
{#-
    Calculate NICE IND214 using paired SMI population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the existing detail columns.
-#}
-- NICE IND214: https://www.nice.org.uk/indicators/ind214
-- Cervical screening completed in 5.5 years for women aged 50 to 64 with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.birth_date_approx,
        population.practice_code,
        population.practice_name,
        screening.latest_completed_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_cervical_screening_evidence', reference) }} AS screening
        ON population.person_id = screening.person_id
        AND population.reporting_date = screening.reporting_date
    WHERE profile.has_active_smi_diagnosis
        AND population.gender = 'Female'
        AND population.age BETWEEN 50 AND 64
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        CASE WHEN population.latest_completed_date >= DATEADD(month, -66, population.reporting_date) THEN population.latest_completed_date END AS latest_record_date,
        COALESCE(population.latest_completed_date >= DATEADD(month, -66, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND214' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: cervical screening (50 to 64 years)' AS indicator_name,
    'The percentage of women aged 50 or over and who have not attained the age of 65 with schizophrenia, bipolar affective disorder and other psychoses whose notes record that a cervical screening test has been performed in the preceding 5 years and 6 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -66, reporting_date) AS measurement_period_start,
    age,
    'Active severe mental illness, women aged 50 to 64' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
