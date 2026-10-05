{{ config(tags=['monthly-full', 'nice-history']) }}

{% set q197 = namespace(sql=nice_ind197('by_month')) %}
{% set q197.sql = q197.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q197.sql = q197.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q197.sql = q197.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q197.sql = q197.sql | replace(ref('int_nice_alcohol_screen_intervention') | string, 'synthetic_pairs') %}
{% set q197.sql = q197.sql | replace(ref('int_alcohol_intervention') | string, 'synthetic_interventions') %}
{% set q199 = namespace(sql=nice_ind199('by_month')) %}
{% set q199.sql = q199.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q199.sql = q199.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q199.sql = q199.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q199.sql = q199.sql | replace(ref('int_nice_alcohol_screen_intervention') | string, 'synthetic_pairs') %}
{% set q199.sql = q199.sql | replace(ref('int_alcohol_intervention') | string, 'synthetic_interventions') %}
{% set q200 = namespace(sql=nice_ind200('by_month')) %}
{% set q200.sql = q200.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q200.sql = q200.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q200.sql = q200.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q200.sql = q200.sql | replace(ref('int_nice_alcohol_screen_intervention') | string, 'synthetic_pairs') %}
{% set q200.sql = q200.sql | replace(ref('int_alcohol_intervention') | string, 'synthetic_interventions') %}
{% set q202 = namespace(sql=nice_ind202('by_month')) %}
{% set q202.sql = q202.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set q202.sql = q202.sql | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set q202.sql = q202.sql | replace(nice_ref('int_nice_alcohol_evidence', 'by_month') | string, 'synthetic_evidence') %}
{% set q202.sql = q202.sql | replace(ref('int_nice_alcohol_screen_intervention') | string, 'synthetic_pairs') %}
{% set q202.sql = q202.sql | replace(ref('int_alcohol_intervention') | string, 'synthetic_interventions') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        40 AS age, 'SYN_PRACTICE' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES (-9821, '2024-01-31'), (-9821, '2024-02-29'), (-9821, '2025-01-31'), (-9821, '2025-02-28'),
        (-9822, '2024-01-31'), (-9822, '2024-03-31'),
        (-9823, '2024-05-31'), (-9824, '2024-05-31'),
        (-9825, '2025-01-31'), (-9825, '2025-02-28'),
        (-9825, '2026-01-31'), (-9825, '2026-02-28')
),
synthetic_ltc AS (
    SELECT person_id, reporting_date,
        CASE WHEN person_id = -9825 THEN '2020-01-01'
            WHEN person_id = -9821 THEN '2023-12-01'
            ELSE '2024-01-01' END::DATE AS earliest_hypertension_date,
        -- The positive screen expires while this diagnosis still qualifies for IND199.
        CASE WHEN person_id = -9825 THEN '2024-12-01'
            WHEN person_id = -9821 THEN '2023-12-01'
            ELSE '2024-01-01' END::DATE AS earliest_depression_anxiety_date,
        FALSE AS has_nice_alcohol_disorder, TRUE AS has_active_smi_diagnosis,
        '2020-01-01'::DATE AS earliest_smi_diagnosis_date,
        TRUE AS has_chd, FALSE AS has_atrial_fibrillation, FALSE AS has_heart_failure,
        FALSE AS has_stroke_tia, FALSE AS has_diabetes, FALSE AS has_dementia
    FROM synthetic_population
),
synthetic_evidence AS (
    SELECT person_id, reporting_date,
        IFF(person_id = -9825, '2024-01-31'::DATE, reporting_date) AS latest_alcohol_screen_date,
        'FAST' AS latest_alcohol_screen_tool, 3::FLOAT AS latest_alcohol_screen_score,
        CASE WHEN person_id = -9825 THEN '2024-01-31'
            WHEN person_id = -9821 AND reporting_date >= '2025-01-01' THEN '2025-01-01'
            ELSE '2024-01-01' END::DATE AS latest_positive_alcohol_screen_date,
        NULL::DATE AS latest_intervention_after_positive_screen_date
    FROM synthetic_population
),
synthetic_pairs AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS screen_date,
        column3::DATE AS first_intervention_date, column4::DATE AS latest_intervention_date
    FROM VALUES (-9821, '2023-01-31', '2023-04-30', '2023-04-30'),
        (-9821, '2024-01-01', NULL, NULL), (-9821, '2025-01-01', NULL, NULL),
        (-9822, '2024-01-01', '2024-01-15', '2024-03-01'),
        (-9823, '2024-01-01', '2024-04-01', '2024-04-01'),
        (-9824, '2024-01-01', NULL, NULL)
),
synthetic_interventions AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS clinical_effective_date,
        'Yes' AS alcohol_advice_services
    FROM VALUES (-9821, '2023-04-30'), (-9822, '2024-01-15'), (-9822, '2024-03-01'),
        (-9823, '2024-04-01'), (-9823, '2024-04-02'), (-9824, '2024-04-02')
),

actual_197 AS ({{ q197.sql }}),

actual_199 AS ({{ q199.sql }}),

actual_200 AS ({{ q200.sql }}),

actual_202 AS ({{ q202.sql }})

SELECT '197' AS rule, COUNT(*) AS rows_total
FROM actual_197
HAVING EXISTS (SELECT 1 FROM actual_197
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 6
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 5
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-01-31' AND latest_record_date = '2024-01-15'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-03-31' AND latest_record_date = '2024-03-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9823 AND latest_record_date = '2024-04-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9824 AND NOT is_in_numerator AND latest_record_date IS NULL), 0) <> 1
UNION ALL
SELECT '199' AS rule, COUNT(*) AS rows_total
FROM actual_199
HAVING EXISTS (SELECT 1 FROM actual_199
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 7
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 4
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-01-31' AND latest_record_date = '2024-01-15'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-03-31' AND latest_record_date = '2024-03-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9823 AND latest_record_date = '2024-04-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9824 AND NOT is_in_numerator AND latest_record_date IS NULL), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9821 AND reporting_date = '2024-02-29' AND is_in_numerator), 0) <> 0
    OR COALESCE(COUNT_IF(person_id = -9825 AND reporting_date = '2025-01-31'
        AND latest_positive_alcohol_screen_date = '2024-01-31'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9825 AND reporting_date > '2025-01-31'), 0) <> 0
UNION ALL
SELECT '200' AS rule, COUNT(*) AS rows_total
FROM actual_200
HAVING EXISTS (SELECT 1 FROM actual_200
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 9
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 4
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-01-31' AND latest_record_date = '2024-01-15'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-03-31' AND latest_record_date = '2024-03-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9823 AND latest_record_date = '2024-04-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9824 AND NOT is_in_numerator AND latest_record_date IS NULL), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9821 AND reporting_date = '2024-02-29' AND is_in_numerator), 0) <> 0
    OR COALESCE(COUNT_IF(person_id = -9825 AND reporting_date = '2025-01-31'
        AND latest_positive_alcohol_screen_date = '2024-01-31'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9825 AND reporting_date > '2025-01-31'), 0) <> 0
UNION ALL
SELECT '202' AS rule, COUNT(*) AS rows_total
FROM actual_202
HAVING EXISTS (SELECT 1 FROM actual_202
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 11
    OR COALESCE(COUNT_IF(is_in_numerator), 0) <> 6
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-01-31' AND latest_record_date = '2024-01-15'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9822 AND reporting_date = '2024-03-31' AND latest_record_date = '2024-03-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9823 AND latest_record_date = '2024-04-01'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9824 AND NOT is_in_numerator AND latest_record_date IS NULL), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9821 AND reporting_date = '2025-02-28' AND is_in_numerator), 0) <> 0
    OR COALESCE(COUNT_IF(person_id = -9825), 0) <> 3
    OR COALESCE(COUNT_IF(person_id = -9825 AND reporting_date = '2026-01-31'
        AND latest_positive_alcohol_screen_date = '2024-01-31'), 0) <> 1
    OR COALESCE(COUNT_IF(person_id = -9825 AND reporting_date > '2026-01-31'), 0) <> 0
