{{ config(tags=['monthly-full', 'nice-history']) }}

{% set register = calculate_copd_register(reference_dates='SELECT reporting_date AS reference_date FROM synthetic_dates') %}
{% set register = register | replace(ref('int_copd_diagnoses_all') | string, 'synthetic_diagnoses') %}
{% set register = register | replace(ref('int_spirometry_all') | string, 'synthetic_spirometry') %}
{% set register = register | replace(ref('dim_person_historical_practice') | string, 'synthetic_registration') %}
{% set query = nice_ind140('by_month') | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query = query | replace(nice_register('COPD', 'by_month') | string, 'SELECT person_id, reference_date AS reporting_date FROM synthetic_register WHERE is_on_register') %}
{% set query = query | replace(ref('int_nice_copd_observations_all') | string, 'synthetic_observations') %}

WITH synthetic_dates AS (
    SELECT column1::DATE AS reporting_date FROM VALUES ('2026-08-31'), ('2026-09-30')
), synthetic_population AS (
    SELECT column1::NUMBER AS person_id, d.reporting_date, 60 AS age,
        'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES (-9801), (-9802), (-9803)
    CROSS JOIN synthetic_dates d
), synthetic_diagnoses AS (
    SELECT column1::NUMBER AS person_id, column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::DATE AS date_recorded, column4::BOOLEAN AS is_disorder_code,
        FALSE AS is_admin_code, NOT column4::BOOLEAN AS is_resolved_code
    FROM VALUES
        (-9801, '2026-08-01', '2026-09-01', TRUE),
        (-9802, '2026-10-01', '2026-08-01', TRUE),
        (-9803, '2020-01-01', '2020-01-01', TRUE),
        (-9803, '2026-08-01', '2026-09-01', FALSE)
), synthetic_spirometry AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS clinical_effective_date,
        NULL::DATE AS date_recorded, FALSE AS is_below_0_7 WHERE FALSE
), synthetic_registration AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS registration_start_date,
        NULL::NUMBER AS practice_id WHERE FALSE
), synthetic_register AS ({{ register }}), synthetic_observations AS (
    SELECT column1::NUMBER AS person_id, '2026-08-01'::DATE AS event_date,
        'FEV1_COD' AS evidence_type FROM VALUES (-9801), (-9802), (-9803)
), actual AS (
    SELECT person_id, reporting_date, is_in_numerator FROM ({{ query }})
), expected AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date, TRUE AS is_in_numerator
    FROM VALUES (-9801, '2026-09-30'), (-9803, '2026-08-31')
), actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS (
    (SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts)
    UNION ALL
    (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
