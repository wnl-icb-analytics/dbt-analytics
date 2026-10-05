{% macro nice_ind82(reference='current') %}
{#-
    Calculate NICE IND82 from the paired population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with indicator detail.
-#}
-- NICE IND82: https://www.nice.org.uk/indicators/ind82
-- Alcohol consumption recorded in 15 months for people with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_alcohol_record_date
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
        population.latest_alcohol_record_date AS latest_alcohol_record_date,
        CASE
            WHEN population.latest_alcohol_record_date >= DATEADD(month, -15, population.reporting_date)
                THEN population.latest_alcohol_record_date
        END AS latest_record_date,
        COALESCE(population.latest_alcohol_record_date >= DATEADD(month, -15, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND82' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: annual record of alcohol consumption' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission)' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_alcohol_record_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
