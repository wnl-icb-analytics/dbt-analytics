{#-
    Calculate NICE IND86 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND86 detail columns, one person per reporting_date.
-#}
{% macro nice_ind86(reference='current') %}
-- NICE IND86: https://www.nice.org.uk/indicators/ind86
-- Creatinine and thyroid function tests within nine months for current lithium users, regardless of SMI diagnosis.
WITH indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    WHERE profile.is_on_lithium
),
daily_tests AS (
    -- Only dates matter; repeated records on the same day are interchangeable.
    SELECT DISTINCT person_id, clinical_effective_date::DATE AS event_date, source_cluster_id
    FROM {{ ref('int_lithium_monitoring_tests_all') }}
),
selected_dates AS (
    SELECT population.person_id, population.reporting_date, kind.source_cluster_id, test.event_date
    FROM indicator_population AS population
    CROSS JOIN (SELECT column1::VARCHAR AS source_cluster_id FROM VALUES ('CRE_COD'), ('TFT_COD')) AS kind
    ASOF JOIN daily_tests AS test
        MATCH_CONDITION (population.reporting_date >= test.event_date)
        ON population.person_id = test.person_id AND kind.source_cluster_id = test.source_cluster_id
),
test_dates AS (
    SELECT person_id, reporting_date,
        MAX(IFF(source_cluster_id = 'CRE_COD', event_date, NULL)) AS latest_creatinine_date,
        MAX(IFF(source_cluster_id = 'TFT_COD', event_date, NULL)) AS latest_thyroid_function_test_date
    FROM selected_dates GROUP BY person_id, reporting_date
),
assessed AS (
    SELECT population.*, test.latest_creatinine_date, test.latest_thyroid_function_test_date,
        COALESCE(test.latest_creatinine_date BETWEEN DATEADD(month, -9, population.reporting_date)
            AND population.reporting_date, FALSE)
        AND COALESCE(test.latest_thyroid_function_test_date BETWEEN DATEADD(month, -9, population.reporting_date)
            AND population.reporting_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN test_dates AS test ON population.person_id = test.person_id
        AND population.reporting_date = test.reporting_date
)
SELECT person_id, 'IND86' AS indicator_id, 'Bipolar, schizophrenia and other psychoses: target organ damage' AS indicator_name,
    reporting_date, DATEADD(month, -9, reporting_date) AS measurement_period_start,
    age, 'Current lithium therapy' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_creatinine_date, latest_thyroid_function_test_date,
    IFF(is_in_numerator, GREATEST(latest_creatinine_date, latest_thyroid_function_test_date), NULL) AS latest_record_date,
    TRUE AS is_in_denominator, is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
