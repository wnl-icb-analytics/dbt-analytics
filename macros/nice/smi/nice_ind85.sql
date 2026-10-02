{% macro nice_ind85(reference='current') %}
{#-
    Calculate NICE IND85 from the paired population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with indicator detail.
-#}
-- NICE IND85: https://www.nice.org.uk/indicators/ind85
-- Cervical screening completed in 5 years for women aged 25 to 64 with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_completed_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_cervical_screening_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.has_active_smi_diagnosis
        AND population.gender = 'Female'
        AND population.age BETWEEN 25 AND 64
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        CASE
            WHEN population.latest_completed_date >= DATEADD(month, -60, population.reporting_date)
                THEN population.latest_completed_date
        END AS latest_record_date,
        COALESCE(population.latest_completed_date >= DATEADD(month, -60, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND85' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: cervical screening' AS indicator_name,
    reporting_date,
    DATEADD(month, -60, reporting_date) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission), women aged 25 to 64' AS condition_name,
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
