{% macro nice_ind212(reference='current') %}
{#-
    Calculate NICE IND212 for very severe COPD register members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND212 detail columns, one person per reporting_date.
-#}
-- NICE IND212: https://www.nice.org.uk/indicators/ind212
WITH population AS (
    SELECT p.*
    FROM ({{ nice_reference_population(reference) }}) p
    INNER JOIN ({{ nice_register('COPD', reference) }}) r
        ON p.person_id = r.person_id AND p.reporting_date = r.reporting_date
), valid_percentages AS (
    -- Filter valid percentages before selecting the latest date; highest observation id resolves same-day ties.
    SELECT person_id, event_date, result_value
    FROM {{ ref('int_nice_copd_observations_all') }}
    WHERE evidence_type = 'FEV1_PCT_PRED_COD' AND result_value BETWEEN 5 AND 150
        AND LOWER(TRIM(result_unit_display)) IN ('percent', '%')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY observation_id DESC) = 1
), severity AS (
    SELECT p.person_id, p.reporting_date, MAX(e.event_date) AS latest_very_severe_code_date
    FROM population p
    INNER JOIN {{ ref('int_nice_copd_observations_all') }} e
        ON p.person_id = e.person_id AND e.is_very_severe_copd
        AND {{ ltc_register_known_by('e.event_date', 'e.date_recorded', 'p.reporting_date') }}
    GROUP BY p.person_id, p.reporting_date
), selected AS (
    SELECT p.*, e.event_date AS latest_fev1_percent_date, e.result_value AS latest_fev1_percent,
        s.latest_very_severe_code_date
    FROM population p
    ASOF JOIN valid_percentages e MATCH_CONDITION (p.reporting_date >= e.event_date)
        ON p.person_id = e.person_id
    LEFT JOIN severity s ON p.person_id = s.person_id AND p.reporting_date = s.reporting_date
), eligible AS (
    SELECT * FROM selected
    WHERE latest_fev1_percent < 30 OR latest_very_severe_code_date IS NOT NULL
), saturations AS (
    SELECT person_id, event_date, result_value
    FROM {{ ref('int_nice_copd_observations_all') }}
    WHERE evidence_type = 'ARDENS/SPO2_SATS_SATURATIONS' AND result_value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY observation_id DESC) = 1
), assessed AS (
    SELECT p.*, IFF(s.event_date > DATEADD(month, -12, p.reporting_date), s.event_date, NULL)
        AS latest_record_date,
        IFF(s.event_date > DATEADD(month, -12, p.reporting_date), s.result_value, NULL) AS latest_spo2_value
    FROM eligible p
    ASOF JOIN saturations s MATCH_CONDITION (p.reporting_date >= s.event_date)
        ON p.person_id = s.person_id
)
SELECT person_id, 'IND212' AS indicator_id, 'COPD: oxygen saturation recording' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Very severe COPD' AS condition_name, {{ nice_practice_columns('assessed', reference) }},
    latest_fev1_percent_date, latest_fev1_percent, latest_very_severe_code_date,
    latest_spo2_value, latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
