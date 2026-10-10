{{ config(tags=['monthly-full', 'nice-history']) }}

{% set q207 = namespace(sql=nice_ind207('by_month')) %}
{% set q207.sql = q207.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q207.sql = q207.sql | replace(nice_ref('int_nice_multimorbidity_categories', 'by_month') | string, 'synthetic_categories') %}
{% set q207.sql = q207.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q207.sql = q207.sql | replace(nice_ref('int_nice_review_evidence', 'by_month') | string, 'synthetic_review') %}
{% set q208 = namespace(sql=nice_ind208('by_month')) %}
{% set q208.sql = q208.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q208.sql = q208.sql | replace(nice_ref('int_nice_multimorbidity_categories', 'by_month') | string, 'synthetic_categories') %}
{% set q208.sql = q208.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q208.sql = q208.sql | replace(nice_ref('int_nice_review_evidence', 'by_month') | string, 'synthetic_review') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        70 AS age, 'SYN_PRACTICE' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES (-9801, '2024-01-31'), (-9801, '2024-02-29'), (-9801, '2025-01-31'), (-9801, '2025-02-28'),
        (-9802, '2024-01-31'), (-9802, '2024-02-29'), (-9802, '2025-01-31'), (-9802, '2025-02-28')
),
synthetic_categories AS (
    SELECT person_id, reporting_date, category.column1::VARCHAR AS category_code
    FROM synthetic_population
    CROSS JOIN (SELECT column1 FROM VALUES ('SYN_A'), ('SYN_B'), ('SYN_C'), ('SYN_D')) AS category
),
synthetic_ltc AS (
    SELECT person_id, reporting_date, 1 AS ltc_count, 'Moderate' AS latest_frailty_severity
    FROM synthetic_population WHERE person_id = -9801
),
synthetic_review AS (
    SELECT person_id, reporting_date, '2024-01-31'::DATE AS latest_structured_medication_review_date,
        '2024-01-31'::DATE AS latest_falls_discussion_date
    FROM synthetic_population
),

actual_207 AS ({{ q207.sql }}),

actual_208 AS ({{ q208.sql }})

SELECT '207' AS rule, COUNT(*) AS rows_total
FROM actual_207
HAVING EXISTS (SELECT 1 FROM actual_207
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 8
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 6
    OR COALESCE(COUNT_IF(reporting_date = '2025-02-28' AND latest_record_date IS NOT NULL), 0) <> 0
    OR COALESCE(COUNT_IF(person_id = -9802 AND ltc_count = 0 AND multimorbidity_cluster_count = 4), 0) <> 4
UNION ALL
SELECT '208' AS rule, COUNT(*) AS rows_total
FROM actual_208
HAVING EXISTS (SELECT 1 FROM actual_208
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 4
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 3
    OR COALESCE(COUNT_IF(reporting_date = '2025-02-28' AND latest_record_date IS NOT NULL), 0) <> 0
