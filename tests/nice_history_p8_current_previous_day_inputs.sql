{{ config(tags=['monthly-full', 'nice-history']) }}

{% set q196 = namespace(sql=nice_ind196('current')) %}
{% set q196.sql = q196.sql | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
{% set q196.sql = q196.sql | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set q196.sql = q196.sql | replace(ref('int_nice_alcohol_evidence') | string, 'synthetic_alcohol') %}
{% set q196.sql = q196.sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screen') %}
{% set q196.sql = q196.sql | replace(ref('int_nice_multimorbidity_categories') | string, 'synthetic_categories') %}
{% set q196.sql = q196.sql | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set q207 = namespace(sql=nice_ind207('current')) %}
{% set q207.sql = q207.sql | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
{% set q207.sql = q207.sql | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set q207.sql = q207.sql | replace(ref('int_nice_alcohol_evidence') | string, 'synthetic_alcohol') %}
{% set q207.sql = q207.sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screen') %}
{% set q207.sql = q207.sql | replace(ref('int_nice_multimorbidity_categories') | string, 'synthetic_categories') %}
{% set q207.sql = q207.sql | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}

-- Previous-day inputs must join at today's date for a completed diagnosis window.
-- The recent diagnosis with a screen and the one without a screen stay absent.
WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date,
        70 AS age, 'SYN_PRACTICE' AS practice_code, 'Synthetic practice' AS practice_name
    FROM VALUES (-9831), (-9832), (-9833)
),
synthetic_ltc AS (
    SELECT person_id, DATEADD(day, -1, reporting_date) AS reporting_date,
        1 AS ltc_count, 'Moderate' AS latest_frailty_severity,
        IFF(person_id = -9832, DATEADD(month, -3, CURRENT_DATE()),
            DATEADD(month, -1, CURRENT_DATE()))::DATE AS earliest_hypertension_date,
        FALSE AS has_nice_alcohol_disorder
    FROM synthetic_population
),
synthetic_alcohol AS (
    SELECT person_id, DATEADD(day, -1, reporting_date) AS reporting_date,
        DATEADD(day, -2, CURRENT_DATE())::DATE AS latest_alcohol_screen_date,
        'FAST' AS latest_alcohol_screen_tool, 3::FLOAT AS latest_alcohol_screen_score,
        DATEADD(day, -2, CURRENT_DATE())::DATE AS latest_positive_alcohol_screen_date,
        NULL::DATE AS latest_intervention_after_positive_screen_date
    FROM synthetic_population
),
synthetic_screen AS (
    SELECT person_id, latest_alcohol_screen_date AS clinical_effective_date, 'FAST' AS screening_tool
    FROM synthetic_alcohol
    WHERE person_id <> -9833
),
synthetic_categories AS (
    SELECT person_id, DATEADD(day, -1, reporting_date) AS reporting_date, 'SYN_CATEGORY' AS category_code
    FROM synthetic_population
),
synthetic_review AS (
    SELECT person_id, DATEADD(day, -1, reporting_date) AS reporting_date,
        DATEADD(day, -2, CURRENT_DATE())::DATE AS latest_structured_medication_review_date
    FROM synthetic_population
),
actual_196 AS ({{ q196.sql }}),
actual_207 AS ({{ q207.sql }})
SELECT 'IND196' AS rule, COUNT(*) AS rows_total
FROM actual_196
HAVING EXISTS (SELECT 1 FROM actual_196
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 1 OR COALESCE(COUNT_IF(is_in_numerator AND reporting_date = CURRENT_DATE()
    AND person_id = -9832
    AND new_diagnosis_date = DATEADD(month, -3, CURRENT_DATE())
    AND latest_alcohol_screen_date = DATEADD(day, -2, CURRENT_DATE())),0) <> 1
UNION ALL
SELECT 'IND207', COUNT(*)
FROM actual_207
HAVING EXISTS (SELECT 1 FROM actual_207
    GROUP BY person_id, reporting_date HAVING COUNT(*) <> 1)
    OR COUNT(*) <> 3 OR COALESCE(COUNT_IF(is_in_numerator AND reporting_date = CURRENT_DATE()
    AND ltc_count = 1 AND multimorbidity_cluster_count = 1),0) <> 3
