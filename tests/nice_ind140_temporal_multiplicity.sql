{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = nice_ind140('by_month') %}
{% set query = query | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_nice_reference_population') %}
{% set query = query | replace(nice_register('COPD', 'by_month') | string, 'SELECT * FROM synthetic_nice_register') %}
{% set query = query | replace(ref('int_nice_copd_observations_all') | string, 'synthetic_events_0') %}

WITH synthetic_nice_reference_population AS (SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date, column3::NUMBER AS age, column4::VARCHAR AS practice_code, column5::VARCHAR AS practice_name FROM VALUES (-9711,'2026-08-31',60,'SYNTHETIC','Synthetic practice'), (-9711,'2026-09-30',60,'SYNTHETIC','Synthetic practice'), (-9712,'2026-08-31',60,'SYNTHETIC','Synthetic practice'), (-9712,'2026-09-30',60,'SYNTHETIC','Synthetic practice'), (-9713,'2026-08-31',60,'SYNTHETIC','Synthetic practice'), (-9713,'2026-09-30',60,'SYNTHETIC','Synthetic practice')),
synthetic_nice_register AS (SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date, column3::DATE AS earliest_diagnosis_date FROM VALUES (-9711,'2026-08-31','2020-01-01'), (-9711,'2026-09-30','2020-01-01'), (-9712,'2026-08-31','2020-01-01'), (-9712,'2026-09-30','2020-01-01'), (-9713,'2026-08-31','2020-01-01'), (-9713,'2026-09-30','2020-01-01')),
synthetic_events_0 AS (SELECT column1::NUMBER AS person_id, column2::NUMBER AS observation_id, column3::DATE AS event_date, column4::DATE AS date_recorded, column5::VARCHAR AS evidence_type, column6::FLOAT AS result_value, column7::VARCHAR AS result_unit_display, column8::BOOLEAN AS is_very_severe_copd FROM VALUES (-9711,1,'2025-09-30','2025-09-30','FEV1_COD',NULL,NULL,FALSE), (-9712,2,'2026-09-30','2026-09-30','FEV1_COD',NULL,NULL,FALSE), (-9712,3,'2026-10-01','2026-10-01','FEV1_COD',NULL,NULL,FALSE), (-9713,4,'2026-10-01','2026-10-01','FEV1_COD',NULL,NULL,FALSE)),
actual AS (SELECT person_id, reporting_date, is_in_numerator FROM ({{ query }})),
expected AS (SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date, column3::BOOLEAN AS is_in_numerator FROM VALUES (-9711,'2026-08-31',TRUE), (-9711,'2026-09-30',FALSE), (-9712,'2026-08-31',FALSE), (-9712,'2026-09-30',TRUE), (-9713,'2026-08-31',FALSE), (-9713,'2026-09-30',FALSE)),
actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS ((SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts) UNION ALL (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts))
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
