{% macro nice_asthma_diagnosis_population(reference='current') %}
WITH reference_dates AS (
    SELECT reporting_date AS reference_date FROM ({{ nice_reference_dates(reference) }})
),
{% if reference == 'current' %}
diagnosis_state AS (
    SELECT person_id, CURRENT_DATE()::DATE AS reporting_date,
        MAX(IFF(is_diagnosis_code, clinical_effective_date::DATE, NULL)) AS diagnosis_date,
        MAX(IFF(is_resolved_code, clinical_effective_date::DATE, NULL)) AS resolved_date
    FROM {{ ref('int_asthma_diagnoses_all') }}
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    GROUP BY person_id
)
{% else %}
keyed_events AS (
    -- Calendar-date ordering preserves IND273's reviewed same-day resolution rule.
    SELECT person_id, clinical_effective_date::DATE AS clinical_date,
        is_diagnosis_code,
        TO_CHAR(clinical_effective_date::DATE, 'YYYYMMDD') || IFF(is_resolved_code, '1', '0') AS record_key,
        {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date
    FROM {{ ref('int_asthma_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
),
selected_keys AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, MIN(known_date) AS known_date FROM keyed_events GROUP BY person_id, record_key', 'reference_dates') }}
),
diagnosis_state AS (
    SELECT person_id, reference_date AS reporting_date,
        IFF(RIGHT(record_key, 1) = '0', TO_DATE(LEFT(record_key, 8), 'YYYYMMDD'), NULL) AS diagnosis_date,
        IFF(RIGHT(record_key, 1) = '1', TO_DATE(LEFT(record_key, 8), 'YYYYMMDD'), NULL) AS resolved_date
    FROM selected_keys
)
{% endif %}
SELECT population.person_id, population.reporting_date, population.age,
    population.birth_date_approx, population.gender, population.practice_code, population.practice_name
FROM ({{ nice_reference_population(reference) }}) population
INNER JOIN diagnosis_state state ON population.person_id = state.person_id
    AND population.reporting_date = state.reporting_date
WHERE population.age >= 5 AND state.diagnosis_date IS NOT NULL
    AND (state.resolved_date IS NULL OR state.diagnosis_date > state.resolved_date)
{% endmacro %}
