{{ config(tags=['monthly-full', 'nice-history']) }}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, '2026-09-30'::DATE AS reporting_date,
        column2::NUMBER AS age, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-701, 0), (-702, 17), (-703, 18), (-704, 19), (-705, NULL)
),
synthetic_register AS (
    SELECT person_id, reporting_date, '2026-04-01'::DATE AS earliest_diagnosis_date
    FROM synthetic_population
),
synthetic_ecg AS (
    SELECT person_id, '2026-04-01'::DATE AS clinical_effective_date,
        'ECG_12LEAD_COD' AS cluster_id FROM synthetic_population
),
synthetic_urine AS (
    SELECT person_id, clinical_effective_date, 'URINE_BLOOD_TEST_COD' AS cluster_id
    FROM synthetic_ecg
),
synthetic_acr AS (
    SELECT person_id, clinical_effective_date, TRUE AS is_acr_ratio FROM synthetic_ecg
),
actual AS (
    {% for reference in ['current', 'by_month'] %}
        {% for indicator in [121, 122, 123] %}
            {% set query = context['nice_ind' ~ indicator](reference) %}
            {% set query = query | replace(nice_reference_population(reference) | string, 'SELECT * FROM synthetic_population') %}
            {% set query = query | replace(nice_register('HTN', reference) | string, 'SELECT * FROM synthetic_register') %}
            {% set query = query | replace(ref('int_ecg_all') | string, 'synthetic_ecg') %}
            {% set query = query | replace(ref('int_urine_blood_test_all') | string, 'synthetic_urine') %}
            {% set query = query | replace(ref('int_urine_acr_all') | string, 'synthetic_acr') %}
            SELECT '{{ reference }}' AS reference, person_id, reporting_date, age,
                indicator_id, is_in_denominator, is_in_numerator
            FROM ({{ query }})
            {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
expected AS (
    SELECT modes.reference, population.person_id, population.reporting_date,
        population.age, indicators.indicator_id,
        TRUE AS is_in_denominator, TRUE AS is_in_numerator
    FROM synthetic_population AS population
    CROSS JOIN (SELECT column1::VARCHAR AS reference FROM VALUES ('current'), ('by_month')) AS modes
    CROSS JOIN (SELECT column1::VARCHAR AS indicator_id FROM VALUES ('IND121'), ('IND122'), ('IND123')) AS indicators
    WHERE indicators.indicator_id IN ('IND122', 'IND123') OR population.age > 18
),
actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS (
    (SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts)
    UNION ALL
    (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
