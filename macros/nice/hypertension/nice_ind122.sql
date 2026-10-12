{% macro nice_ind122(reference='current') %}
{#-
    Calculate NICE IND122 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND122 detail columns, one person per reporting_date.
-#}
-- NICE IND122: https://www.nice.org.uk/indicators/ind122
-- Haematuria test within three months either side of diagnosis, at any age.
-- The rolling annual diagnosis cohort ends three months before the reporting date.
WITH population AS (
    {{ nice_hypertension_new_diagnosis_population(reference) }}
),

evidence AS (
    SELECT person_id, clinical_effective_date::DATE AS test_date, cluster_id
    FROM {{ ref('int_urine_blood_test_all') }}
),

selected AS (
    SELECT population.person_id, population.reporting_date, MAX(evidence.test_date) AS latest_haematuria_test_date,
        COALESCE(COUNT_IF(evidence.cluster_id = 'URINE_BLOOD_TEST_COD'), 0) > 0 AS has_blood_specific_test
    FROM population
    LEFT JOIN evidence
        ON population.person_id = evidence.person_id
        AND evidence.test_date BETWEEN DATEADD(month, -3, population.diagnosis_date)
            AND LEAST(DATEADD(month, 3, population.diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
)

SELECT
    population.person_id,
    'IND122' AS indicator_id,
    'Hypertension: haematuria for target organ damage' AS indicator_name,
    'The percentage of patients with a new diagnosis of hypertension in the preceding 1 April to 31 March who have a record of a test for haematuria in the 3 months before or after the date of entry to the hypertension register.' AS indicator_description,
    population.reporting_date,
    DATEADD(month, -15, population.reporting_date) AS measurement_period_start,
    population.age,
    'New hypertension, any age'::VARCHAR(59) AS denominator_description,
    {{ nice_practice_columns('population', reference) }},
    population.diagnosis_date,
    selected.latest_haematuria_test_date,
    selected.has_blood_specific_test,
    TRUE AS is_in_denominator,
    selected.latest_haematuria_test_date IS NOT NULL AS is_in_numerator,
    IFF(selected.latest_haematuria_test_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM population
LEFT JOIN selected
    ON population.person_id = selected.person_id
    AND population.reporting_date = selected.reporting_date
{% endmacro %}
