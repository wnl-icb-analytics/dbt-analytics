{#-
    Calculate NICE IND189 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND189 detail columns, one person per reporting_date.
-#}
{% macro nice_ind189(reference='current') %}
-- NICE IND189: https://www.nice.org.uk/indicators/ind189
-- Smoking status recorded within twelve months for unresolved asthma diagnosis members aged 5 to 19.
WITH population AS (
    SELECT * FROM ({{ nice_asthma_diagnosis_population(reference) }})
    WHERE age BETWEEN 5 AND 19
), daily AS (
    SELECT person_id, event_date
    FROM {{ ref('int_nice_smoking_recording_all') }}
    GROUP BY person_id, event_date
), evidence AS (
    SELECT p.*, e.event_date
    FROM population p
    ASOF JOIN daily e MATCH_CONDITION (p.reporting_date >= e.event_date)
        ON p.person_id = e.person_id
), assessed AS (
    SELECT *, IFF(event_date > DATEADD(month, -12, reporting_date), event_date, NULL) AS latest_record_date
    FROM evidence
)
SELECT a.person_id, 'IND189' AS indicator_id,
    'Asthma: smoking status (under 19)' AS indicator_name,
    a.reporting_date, DATEADD(month, -12, a.reporting_date) AS measurement_period_start,
    a.age, 'Asthma (aged 5 to 19)' AS condition_name,
    {{ nice_practice_columns('a', reference) }},
    a.latest_record_date, TRUE AS is_in_denominator,
    a.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(a.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed a
{% endmacro %}
