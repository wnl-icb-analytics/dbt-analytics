{% macro nice_ind159(reference='current') %}
{#-
    Calculate NICE IND159 using paired SMI population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the existing detail columns.
-#}
-- NICE IND159: https://www.nice.org.uk/indicators/ind159
-- Blood glucose or HbA1c recorded in 12 months for adults with an active SMI diagnosis, excluding diabetes diagnosed more than 12 months ago.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.birth_date_approx,
        population.practice_code,
        population.practice_name,
        physical.latest_glucose_or_hba1c_date,
        profile.earliest_diabetes_diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_physical_health_evidence', reference) }} AS physical
        ON population.person_id = physical.person_id
        AND population.reporting_date = physical.reporting_date
    WHERE profile.has_active_smi_diagnosis
        AND population.age >= 18
        AND NOT COALESCE(profile.earliest_diabetes_diagnosis_date < DATEADD(month, -12, population.reporting_date), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_glucose_or_hba1c_date AS latest_glucose_or_hba1c_date,
        CASE WHEN population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, population.reporting_date) THEN population.latest_glucose_or_hba1c_date END AS latest_record_date,
        COALESCE(population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND159' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: annual blood glucose or HbA1c' AS indicator_name,
    'The percentage of patients aged 18 years and over with schizophrenia, bipolar affective disorder and other psychoses who have a record of blood glucose or HbA1c in the preceding 12 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Active severe mental illness, aged 18 or over' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_glucose_or_hba1c_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
