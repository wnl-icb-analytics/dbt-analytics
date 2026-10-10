{% macro nice_ind160(reference='current') %}
{#-
    Calculate NICE IND160 at each reference date using its reviewed rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND160: https://www.nice.org.uk/indicators/ind160
-- Monofilament foot sensation testing in 12 months on the diabetes register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
),

candidate_people AS (
    SELECT DISTINCT person_id
    FROM indicator_population
),

qualifying_days AS (
    SELECT
        obs.person_id,
        obs.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_foot_examination_all') }} AS obs
    INNER JOIN candidate_people AS candidate
        ON obs.person_id = candidate.person_id
    WHERE obs.has_monofilament_test
    GROUP BY obs.person_id, obs.clinical_effective_date::DATE
),

selected_record AS (
    SELECT
        population.person_id,
        population.reporting_date,
        record.event_date
    FROM indicator_population AS population
    ASOF JOIN qualifying_days AS record
        MATCH_CONDITION (population.reporting_date >= record.event_date)
        ON population.person_id = record.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        CASE WHEN record.event_date >= DATEADD(month, -12, population.reporting_date)
            THEN record.event_date END AS latest_record_date
    FROM indicator_population AS population
    LEFT JOIN selected_record AS record
        ON population.person_id = record.person_id
        AND population.reporting_date = record.reporting_date
)

SELECT
    person_id,
    'IND160' AS indicator_id,
    'Diabetes: annual examination of foot sensation' AS indicator_name,
    'The percentage of patients with diabetes with a record of testing of foot sensation using a 10 g monofilament within the preceding 12 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Diabetes' AS denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_record_date,
    TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    CASE
        WHEN latest_record_date IS NOT NULL THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
