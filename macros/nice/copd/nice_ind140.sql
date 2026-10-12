{% macro nice_ind140(reference='current') %}
{#-
    Calculate NICE IND140 for COPD register members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND140 detail columns, one person per reporting_date.
-#}
-- NICE IND140: https://www.nice.org.uk/indicators/ind140
WITH population AS (
    SELECT p.*
    FROM ({{ nice_reference_population(reference) }}) p
    INNER JOIN ({{ nice_register('COPD', reference) }}) r
        ON p.person_id = r.person_id AND p.reporting_date = r.reporting_date
), daily AS (
    SELECT person_id, event_date FROM {{ ref('int_nice_copd_observations_all') }}
    WHERE evidence_type = 'FEV1_COD'
    GROUP BY person_id, event_date
), selected AS (
    SELECT p.*, e.event_date
    FROM population p
    ASOF JOIN daily e MATCH_CONDITION (p.reporting_date >= e.event_date)
        ON p.person_id = e.person_id
), assessed AS (
    SELECT *, IFF(event_date > DATEADD(month, -12, reporting_date), event_date, NULL)
        AS latest_record_date
    FROM selected
)
SELECT person_id, 'IND140' AS indicator_id, 'COPD: FEV1' AS indicator_name,
'The percentage of patients with COPD with a record of FEV1 in the preceding 12 months.' AS indicator_description,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'COPD' AS denominator_description, {{ nice_practice_columns('assessed', reference) }},
    latest_record_date AS latest_fev1_date, latest_record_date,
    TRUE AS is_in_denominator, latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
