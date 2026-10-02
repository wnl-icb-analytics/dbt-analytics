{% macro nice_ind232(reference='current') %}
-- NICE IND232: https://www.nice.org.uk/indicators/ind232
WITH indicator_population AS (
    SELECT population.*
    FROM ({{ nice_reference_population(reference) }}) AS population
    LEFT JOIN ({{ nice_register('CKD', reference) }}) AS ckd
        ON population.person_id = ckd.person_id
        AND population.reporting_date = ckd.reporting_date
    WHERE ckd.person_id IS NULL
),

long_term_users AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        COUNT(DISTINCT orders.order_date::DATE) AS nsaid_issue_day_count,
        MAX(orders.order_date::DATE) AS latest_therapy_order_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_oral_nsaid_medications_all') }} AS orders
        ON population.person_id = orders.person_id
        AND orders.order_date::DATE > DATEADD(month, -24, population.reporting_date)
        AND orders.order_date::DATE <= population.reporting_date
    GROUP BY population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name
    -- Co-prescribed items and duplicate orders on the same day count once.
    HAVING COUNT(DISTINCT orders.order_date::DATE) >= 12
),

egfr_days AS (
    SELECT person_id, clinical_effective_date::DATE AS record_date
    FROM {{ ref('int_egfr_test_all') }}
    -- A performed test counts even when no numeric result is recorded.
    GROUP BY person_id, clinical_effective_date::DATE
),

assessed AS (
    SELECT
        users.*,
        CASE WHEN egfr.record_date >= DATEADD(month, -12, users.reporting_date)
            THEN egfr.record_date END AS latest_record_date
    FROM long_term_users AS users
    ASOF JOIN egfr_days AS egfr
        MATCH_CONDITION (users.reporting_date >= egfr.record_date)
        ON users.person_id = egfr.person_id
)

SELECT
    person_id,
    'IND232' AS indicator_id,
    'Kidney conditions: eGFR for long-term NSAID use' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Long-term oral NSAID use without CKD register membership' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    nsaid_issue_day_count,
    latest_therapy_order_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
