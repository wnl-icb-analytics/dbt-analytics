{% macro nice_ind316(reference='current') %}
{#-
    Calculate NICE IND316 for higher-risk asthma members aged 12+ at each reference date.
    Args: reference is current or by_month.
    Returns: the IND316 detail columns, one person per reporting_date.
-#}
-- NICE IND316: https://www.nice.org.uk/indicators/ind316
WITH population AS (
    SELECT p.*, r.risk_period_start, r.risk_period_end, r.saba_inhaler_count,
        r.oral_steroid_course_count, r.latest_asthma_admission_date,
        r.has_high_saba_use, r.has_repeated_oral_steroids, r.has_asthma_admission
    FROM ({{ nice_asthma_diagnosis_population(reference) }}) p
    INNER JOIN {{ nice_ref('int_nice_asthma_risk', reference) }} r
        ON p.person_id = r.person_id AND p.reporting_date = r.reporting_date
    WHERE p.age >= 12 AND r.is_higher_risk
), orders AS (
    SELECT person_id, order_date FROM {{ ref('int_nice_mart_medications_all') }}
    GROUP BY person_id, order_date
), regimens AS (
    SELECT person_id, event_date FROM {{ ref('int_nice_mart_all') }}
    GROUP BY person_id, event_date
), selected AS (
    SELECT p.*, o.order_date, m.event_date AS latest_mart_date
    FROM population p
    ASOF JOIN orders o MATCH_CONDITION (p.reporting_date >= o.order_date)
        ON p.person_id = o.person_id
    ASOF JOIN regimens m MATCH_CONDITION (p.reporting_date >= m.event_date)
        ON p.person_id = m.person_id
), assessed AS (
    SELECT *, IFF(order_date > DATEADD(month, -12, reporting_date), order_date, NULL)
        AS latest_therapy_order_date,
        COALESCE(order_date > DATEADD(month, -12, reporting_date)
            AND latest_mart_date IS NOT NULL, FALSE) AS is_in_numerator
    FROM selected
)
SELECT person_id, 'IND316' AS indicator_id, 'Asthma: MART (higher risk patients)' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Higher-risk asthma, aged 12 or over' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    risk_period_start, risk_period_end, saba_inhaler_count, oral_steroid_course_count,
    latest_asthma_admission_date, has_high_saba_use, has_repeated_oral_steroids, has_asthma_admission,
    latest_therapy_order_date, latest_mart_date,
    IFF(is_in_numerator, latest_therapy_order_date, NULL) AS latest_record_date,
    TRUE AS is_in_denominator, is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_TREATED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
