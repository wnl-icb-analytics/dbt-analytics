{{ config(tags=['monthly-full', 'nice-history']) }}

{% set register = calculate_hypertension_register(reference_dates='SELECT reporting_date AS reference_date FROM synthetic_dates') %}
{% set register = register | replace(ref('int_hypertension_diagnoses_all') | string, 'synthetic_diagnoses') %}
{% set query123 = nice_ind123('by_month') | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query123 = query123 | replace(nice_register('HTN', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set query123 = query123 | replace(ref('int_ecg_all') | string, 'synthetic_ecg') %}
{% set query122 = nice_ind122('by_month') | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query122 = query122 | replace(nice_register('HTN', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% set query122 = query122 | replace(ref('int_urine_blood_test_all') | string, 'synthetic_urine') %}

WITH synthetic_dates AS (
    SELECT column1::DATE AS reporting_date FROM VALUES ('2026-09-30'), ('2026-10-31')
),
synthetic_population AS (
    SELECT column1::NUMBER AS person_id, dates.reporting_date, 50 AS age,
        'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES (-901), (-902), (-903), (-904), (-905), (-906)
    CROSS JOIN synthetic_dates AS dates
),
synthetic_diagnoses AS (
    SELECT column1::NUMBER AS person_id, column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::DATE AS date_recorded, column4::BOOLEAN AS is_diagnosis_code,
        NOT column4::BOOLEAN AS is_resolved_code
    FROM VALUES
        (-901, '2026-04-01', '2026-04-10', TRUE),
        (-901, '2025-03-31', '2026-10-01', TRUE),
        (-902, '2026-08-01', '2026-10-01', TRUE),
        (-903, '2026-10-01', '2026-09-01', TRUE),
        (-904, '2026-08-01', NULL, TRUE),
        (-905, '2026-08-01', '2026-08-01', TRUE),
        (-905, '2026-10-01', '2026-09-01', FALSE),
        (-906, '2026-08-01', '2026-08-01', TRUE),
        (-906, '2026-05-01', '2026-10-01', TRUE)
),
register_calculation AS ({{ register }}),
synthetic_register AS (
    SELECT person_id, reference_date AS reporting_date, earliest_diagnosis_date
    FROM register_calculation WHERE is_on_register
),
synthetic_ecg AS (
    SELECT column1::NUMBER AS person_id, column2::TIMESTAMP_NTZ AS clinical_effective_date,
        'ECG_12LEAD_COD' AS cluster_id
    FROM VALUES (-901, '2026-04-01'), (-902, '2026-08-01'), (-903, '2026-10-01'),
        (-904, '2026-08-01'), (-905, '2026-08-01'), (-904, '2026-08-01'),
        (-906, '2026-08-02')
),
synthetic_urine AS (SELECT person_id, clinical_effective_date, 'URINE_BLOOD_TEST_COD' AS cluster_id FROM synthetic_ecg),
actual AS (
    SELECT person_id, reporting_date, indicator_id, diagnosis_date, is_in_numerator FROM ({{ query123 }})
    UNION ALL
    SELECT person_id, reporting_date, indicator_id, diagnosis_date, is_in_numerator FROM ({{ query122 }})
),
expected AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_id, column4::DATE AS diagnosis_date, column5::BOOLEAN AS is_in_numerator
    FROM VALUES
        (-901, '2026-09-30', 'IND123', '2026-04-01', TRUE),
        (-902, '2026-10-31', 'IND123', '2026-08-01', TRUE),
        (-903, '2026-10-31', 'IND123', '2026-10-01', TRUE),
        (-904, '2026-09-30', 'IND123', '2026-08-01', TRUE),
        (-904, '2026-10-31', 'IND123', '2026-08-01', TRUE),
        (-905, '2026-09-30', 'IND123', '2026-08-01', TRUE),
        (-906, '2026-09-30', 'IND123', '2026-08-01', TRUE),
        (-906, '2026-10-31', 'IND123', '2026-05-01', FALSE),
        (-901, '2026-09-30', 'IND122', '2026-04-01', TRUE),
        (-902, '2026-10-31', 'IND122', '2026-08-01', TRUE),
        (-903, '2026-10-31', 'IND122', '2026-10-01', TRUE),
        (-904, '2026-09-30', 'IND122', '2026-08-01', TRUE),
        (-904, '2026-10-31', 'IND122', '2026-08-01', TRUE),
        (-905, '2026-09-30', 'IND122', '2026-08-01', TRUE),
        (-906, '2026-09-30', 'IND122', '2026-08-01', TRUE),
        (-906, '2026-10-31', 'IND122', '2026-05-01', FALSE)
),
actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS (
    (SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts)
    UNION ALL
    (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
