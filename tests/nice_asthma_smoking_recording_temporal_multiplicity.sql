{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = nice_ind189('by_month') | replace(nice_asthma_diagnosis_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query = query | replace(ref('int_nice_smoking_recording_all') | string, 'synthetic_active_smoking') %}
{% set query = query | replace(ref('int_smoke_exposure_all') | string, 'synthetic_passive_smoking') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, d.reporting_date, 10 AS age,
        'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES (-9751), (-9752), (-9753)
    CROSS JOIN (
        SELECT column1::DATE AS reporting_date FROM VALUES ('2026-08-31'), ('2026-09-30')
    ) d
), synthetic_active_smoking AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS event_date WHERE FALSE
), synthetic_passive_smoking AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS event_date
    FROM VALUES (-9751, '2025-09-30'), (-9752, '2026-09-30'), (-9753, '2026-10-01')
), actual AS (
    SELECT person_id, reporting_date, is_in_numerator FROM ({{ query }})
), expected AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        column3::BOOLEAN AS is_in_numerator
    FROM VALUES
        (-9751, '2026-08-31', TRUE), (-9751, '2026-09-30', FALSE),
        (-9752, '2026-08-31', FALSE), (-9752, '2026-09-30', TRUE),
        (-9753, '2026-08-31', FALSE), (-9753, '2026-09-30', FALSE)
), actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS (
    (SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts)
    UNION ALL
    (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
