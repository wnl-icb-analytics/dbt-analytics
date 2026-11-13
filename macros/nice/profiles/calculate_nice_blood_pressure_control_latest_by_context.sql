{% macro calculate_nice_blood_pressure_control_latest_by_context(reference='current') %}
{# Latest eligible clinical day across contexts, one person/date/context. #}
WITH candidates AS (
    SELECT population.person_id, population.reporting_date, context.column1 AS bp_context_group
    FROM ({{ nice_blood_pressure_register_population(reference) }}) AS population
    CROSS JOIN (VALUES ('CLINIC'), ('HOME'), ('ABPM')) AS context
),

latest_by_context AS (
    SELECT candidate.person_id, candidate.reporting_date, evidence.* EXCLUDE (person_id)
    FROM candidates AS candidate
    ASOF JOIN {{ ref('int_blood_pressure_control_daily_by_context') }} AS evidence
        MATCH_CONDITION (candidate.reporting_date >= evidence.reading_date)
        ON candidate.person_id = evidence.person_id
        AND candidate.bp_context_group = evidence.bp_context_group
    WHERE evidence.reading_date IS NOT NULL
)

SELECT *
FROM latest_by_context
-- Keep every context on the latest day. An older passing context cannot win.
QUALIFY reading_date = MAX(reading_date) OVER (PARTITION BY person_id, reporting_date)
{% endmacro %}
