{{ config(tags=['monthly-full', 'nice-history']) }}

{% set q196 = namespace(sql=nice_ind196('by_month')) %}
{% set q196.sql = q196.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q196.sql = q196.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q196.sql = q196.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q196.sql = q196.sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screen') %}
{% set q198 = namespace(sql=nice_ind198('by_month')) %}
{% set q198.sql = q198.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q198.sql = q198.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q198.sql = q198.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q198.sql = q198.sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screen') %}
{% set q201 = namespace(sql=nice_ind201('by_month')) %}
{% set q201.sql = q201.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q201.sql = q201.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q201.sql = q201.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q201.sql = q201.sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screen') %}

WITH synthetic_population AS (
    SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date,
        16 AS age, 'SYN_PRACTICE' AS practice_code, 'Synthetic practice' AS practice_name
    FROM (SELECT column1 FROM VALUES ('2024-01-31'), ('2024-04-30'), ('2025-01-31'), ('2025-02-28'), ('2026-05-31')) AS dates
    CROSS JOIN (SELECT column1 FROM VALUES (-9810), (-9811)) AS people
),
synthetic_ltc AS (
    SELECT person_id, reporting_date, '2024-01-31'::DATE AS earliest_hypertension_date,
        '2024-01-31'::DATE AS earliest_depression_anxiety_date, FALSE AS has_nice_alcohol_disorder,
        TRUE AS has_chd, FALSE AS has_atrial_fibrillation, FALSE AS has_heart_failure,
        FALSE AS has_stroke_tia, FALSE AS has_diabetes, FALSE AS has_dementia
    FROM synthetic_population
),
synthetic_evidence AS (
    SELECT person_id, reporting_date, NULL::DATE AS latest_alcohol_screen_date,
        NULL::VARCHAR AS latest_alcohol_screen_tool, NULL::FLOAT AS latest_alcohol_screen_score,
        NULL::DATE AS latest_positive_alcohol_screen_date,
        NULL::DATE AS latest_intervention_after_positive_screen_date
    FROM synthetic_population
),
synthetic_screen AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS clinical_effective_date,
        column3::VARCHAR AS screening_tool
    FROM VALUES (-9810, '2023-10-31', 'FAST'), (-9810, '2024-04-30', 'AUDIT-C'),
        (-9810, '2024-05-01', 'FAST'), (-9810, '2024-01-31', 'AUDIT'),
        (-9811, '2023-10-30', 'FAST'), (-9811, '2024-05-01', 'FAST')
),

actual_196 AS ({{ q196.sql }}),

actual_198 AS ({{ q198.sql }}),

actual_201 AS ({{ q201.sql }})

SELECT '196' AS rule, COUNT(*) AS rows_total
FROM actual_196
HAVING EXISTS (SELECT 1 FROM actual_196
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 6
    OR COALESCE(COUNT_IF(reporting_date = '2024-01-31' AND latest_record_date = '2023-10-31'), 0) <> 1
    OR COALESCE(COUNT_IF(reporting_date IN ('2024-04-30', '2025-01-31') AND latest_record_date = '2024-04-30'), 0) <> 2
    OR COALESCE(COUNT_IF(person_id = -9811 AND NOT is_in_numerator AND latest_record_date IS NULL), 0) <> 3
UNION ALL
SELECT '198' AS rule, COUNT(*) AS rows_total
FROM actual_198
HAVING EXISTS (SELECT 1 FROM actual_198
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 6
    OR COALESCE(COUNT_IF(reporting_date = '2024-01-31' AND latest_record_date = '2023-10-31'), 0) <> 1
    OR COALESCE(COUNT_IF(reporting_date IN ('2024-04-30', '2025-01-31') AND latest_record_date = '2024-04-30'), 0) <> 2
    OR COALESCE(COUNT_IF(person_id = -9811 AND NOT is_in_numerator AND latest_record_date IS NULL), 0) <> 3
UNION ALL
SELECT '201' AS rule, COUNT(*) AS rows_total
FROM actual_201
HAVING EXISTS (SELECT 1 FROM actual_201
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 10
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 8
    OR COALESCE(COUNT_IF(reporting_date = '2026-05-31' AND latest_record_date IS NULL AND NOT is_in_numerator), 0) <> 2
