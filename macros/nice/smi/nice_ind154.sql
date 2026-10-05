{% macro nice_ind154(reference='current') %}
{#-
    Calculate NICE IND154 using paired SMI population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the existing detail columns.
-#}
-- NICE IND154: https://www.nice.org.uk/indicators/ind154
-- Smoking status recorded in 12 months for people with an active SMI diagnosis; a never-smoker reaching 26 by the end of the financial year is covered by a never-smoked record made after their 25th birthday and after their earliest SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.birth_date_approx,
        population.practice_code,
        population.practice_name,
        profile.earliest_smi_diagnosis_date,
        smoking.latest_smoking_status_date,
        smoking.latest_smoking_status,
        smoking.latest_never_smoked_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON population.person_id = smoking.person_id
        AND population.reporting_date = smoking.reporting_date
    WHERE profile.has_active_smi_diagnosis
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_smoking_status AS latest_smoking_status,
        population.latest_smoking_status_date AS latest_smoking_status_date,
        CASE WHEN population.latest_smoking_status_date >= DATEADD(month, -12, population.reporting_date) THEN population.latest_smoking_status_date END AS latest_record_date,
        COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, population.reporting_date), FALSE)
        OR COALESCE(
            DATEADD(year, 26, population.birth_date_approx)
                <= {{ nice_financial_year_end('population.reporting_date') }}
            AND population.latest_smoking_status = 'Never Smoked'
            AND population.latest_never_smoked_date > DATEADD(year, 25, population.birth_date_approx)
            AND population.latest_never_smoked_date > population.earliest_smi_diagnosis_date,
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND154' AS indicator_id,
    'Smoking: smoking status of people with bipolar, schizophrenia and other psychoses' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Active severe mental illness' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_smoking_status,
    latest_smoking_status_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
