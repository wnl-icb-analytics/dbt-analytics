{% macro nice_ind83(reference='current') %}
{#-
    Calculate NICE IND83 from the paired population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with indicator detail.
-#}
-- NICE IND83: https://www.nice.org.uk/indicators/ind83
-- BMI recorded in 15 months for people with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_bmi_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_physical_health_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.has_active_smi_diagnosis
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_bmi_date AS latest_bmi_date,
        CASE
            WHEN population.latest_bmi_date >= DATEADD(month, -15, population.reporting_date)
                THEN population.latest_bmi_date
        END AS latest_record_date,
        COALESCE(population.latest_bmi_date >= DATEADD(month, -15, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND83' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: annual BMI recording' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission)' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bmi_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
