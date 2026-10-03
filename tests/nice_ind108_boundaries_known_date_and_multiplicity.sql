{{ config(tags=['monthly-full', 'nice-history']) }}

{% set register = calculate_rheumatoid_arthritis_register(reference_dates='SELECT reporting_date AS reference_date FROM synthetic_dates') %}
{% set register = register | replace(ref('int_rheumatoid_arthritis_diagnoses_all') | string, 'synthetic_diagnoses') %}
{% set register = register | replace(ref('dim_person_birth_death') | string, 'synthetic_births') %}
{% set query = nice_ind108('by_month') | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query = query | replace(nice_register('RA', 'by_month') | string, 'SELECT * FROM synthetic_ra') %}
{% set query = query | replace(nice_register('CHD', 'by_month') | string, 'SELECT * FROM synthetic_chd') %}
{% set query = query | replace(nice_register('STIA', 'by_month') | string, 'SELECT * FROM synthetic_stia') %}
{% set query = query | replace(nice_register('FH', 'by_month') | string, 'SELECT * FROM synthetic_fh') %}
{% set query = query | replace(ref('int_cvd_risk_assessment_all') | string, 'synthetic_assessments') %}

WITH synthetic_dates AS (
    SELECT column1::DATE AS reporting_date FROM VALUES ('2026-09-30'), ('2026-10-31')
),
synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::NUMBER AS age, dates.reporting_date,
        'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES
        (-1, 30), (-2, 84), (-3, 29), (-4, 85), (-5, 50), (-6, 50), (-7, 50), (-8, 50),
        (-9, 50), (-10, 50), (-11, 50), (-12, 50), (-13, 50), (-14, 50), (-15, 50), (-16, 50)
    CROSS JOIN synthetic_dates AS dates
),
synthetic_births AS (
    SELECT DISTINCT person_id, '1970-01-01'::DATE AS birth_date_approx FROM synthetic_population
),
synthetic_diagnoses AS (
    SELECT DISTINCT person_id,
        IFF(person_id = -15, '2026-10-01', '2020-01-01')::TIMESTAMP_NTZ AS clinical_effective_date,
        IFF(person_id = -14, '2026-10-01', '2020-01-01')::DATE AS date_recorded,
        TRUE AS is_diagnosis_code
    FROM synthetic_population WHERE person_id <> -16
),
register_calculation AS ({{ register }}),
synthetic_ra AS (
    SELECT person_id, reference_date AS reporting_date FROM register_calculation WHERE is_on_register
),
synthetic_chd AS (
    SELECT -10 AS person_id, reporting_date FROM synthetic_dates
    UNION ALL SELECT -13, '2026-10-31'::DATE
),
synthetic_stia AS (SELECT -11 AS person_id, reporting_date FROM synthetic_dates),
synthetic_fh AS (SELECT -12 AS person_id, reporting_date FROM synthetic_dates),
synthetic_assessments AS (
    SELECT column1::NUMBER AS person_id, column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::VARCHAR AS id, column4::VARCHAR AS concept_display,
        column5::FLOAT AS risk_score_value, TRUE AS is_qrisk_code
    FROM VALUES
        (-1, '2025-06-30', 'A', 'QRISK2 score', 10),
        (-2, '2026-09-30', 'A', 'QRISK3 score', 20),
        (-5, '2025-06-29', 'A', 'QRISK2 score', 5),
        (-6, '2026-10-01', 'A', 'QRISK3 score', 12),
        (-7, '2026-09-01', 'A', 'QRISK2 assessment', NULL),
        (-8, '2026-09-01', 'A', 'QRISK score', 20),
        (-9, '2026-09-01', 'A', 'QRISK3 score', 10),
        (-9, '2026-09-01', 'Z', 'QRISK3 score', 999),
        (-13, '2026-09-01', 'A', 'QRISK3 score', 15),
        (-14, '2026-09-01', 'A', 'QRISK2 score', 15),
        (-15, '2026-09-01', 'A', 'QRISK3 score', 15)
),
actual AS (
    SELECT person_id, reporting_date, latest_risk_score, latest_risk_score_date,
        latest_risk_assessment_date, is_in_numerator, indicator_status FROM ({{ query }})
),
expected AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        column3::FLOAT AS latest_risk_score, column4::DATE AS latest_risk_score_date,
        column5::DATE AS latest_risk_assessment_date, column6::BOOLEAN AS is_in_numerator,
        IFF(column6::BOOLEAN, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
    FROM VALUES
        (-1, '2026-09-30', 10, '2025-06-30', '2025-06-30', TRUE),
        (-1, '2026-10-31', 10, '2025-06-30', '2025-06-30', FALSE),
        (-2, '2026-09-30', 20, '2026-09-30', '2026-09-30', TRUE),
        (-2, '2026-10-31', 20, '2026-09-30', '2026-09-30', TRUE),
        (-5, '2026-09-30', 5, '2025-06-29', '2025-06-29', FALSE),
        (-5, '2026-10-31', 5, '2025-06-29', '2025-06-29', FALSE),
        (-6, '2026-09-30', NULL, NULL, NULL, FALSE),
        (-6, '2026-10-31', 12, '2026-10-01', '2026-10-01', TRUE),
        (-7, '2026-09-30', NULL, NULL, '2026-09-01', TRUE),
        (-7, '2026-10-31', NULL, NULL, '2026-09-01', TRUE),
        (-8, '2026-09-30', NULL, NULL, NULL, FALSE),
        (-8, '2026-10-31', NULL, NULL, NULL, FALSE),
        (-9, '2026-09-30', NULL, NULL, '2026-09-01', TRUE),
        (-9, '2026-10-31', NULL, NULL, '2026-09-01', TRUE),
        (-13, '2026-09-30', 15, '2026-09-01', '2026-09-01', TRUE),
        (-14, '2026-10-31', 15, '2026-09-01', '2026-09-01', TRUE),
        (-15, '2026-10-31', 15, '2026-09-01', '2026-09-01', TRUE)
),
actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS (
    (SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts)
    UNION ALL
    (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
