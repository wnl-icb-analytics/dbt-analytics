{% macro nice_ind87(reference='current') %}
{#-
    Calculate NICE IND87 from the paired population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with indicator detail.
-#}
-- The 0.4 to 1.0 mmol/L range and latest-result rule are local interpretations; NICE does not define them.
-- NICE IND87: https://www.nice.org.uk/indicators/ind87
-- Latest serum lithium recorded in 4 months and in the 0.4 to 1.0 mmol/L range for people on lithium therapy.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_lithium_level_date,
        evidence.latest_lithium_level,
        evidence.is_latest_lithium_level_in_range
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_physical_health_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.is_on_lithium
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_lithium_level_date AS latest_lithium_level_date,
        population.latest_lithium_level AS latest_lithium_level,
        CASE
            WHEN population.latest_lithium_level_date >= DATEADD(month, -4, population.reporting_date)
                THEN population.latest_lithium_level_date
        END AS latest_record_date,
        COALESCE(population.latest_lithium_level_date >= DATEADD(month, -4, population.reporting_date), FALSE)
            AND population.is_latest_lithium_level_in_range AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND87' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: lithium levels in therapeutic range' AS indicator_name,
    reporting_date,
    DATEADD(month, -4, reporting_date) AS measurement_period_start,
    age,
    'Current lithium therapy' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_lithium_level_date,
    latest_lithium_level,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_record_date IS NULL THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN latest_lithium_level IS NULL THEN 'NOT_ASSESSABLE'
        ELSE 'OUT_OF_RANGE'
    END AS indicator_status
FROM assessed
{% endmacro %}
