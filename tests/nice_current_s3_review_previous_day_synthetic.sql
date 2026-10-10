{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = calculate_nice_review_evidence('current') %}
{% set calculation = calculation | replace(nice_asthma_diagnosis_population('current') | string, 'SELECT person_id, reporting_date FROM synthetic_population WHERE FALSE') %}
{% set calculation = calculation | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
{% set replacements = {'int_nice_ltc_population': 'synthetic_profile', 'int_nice_multimorbidity_categories': 'synthetic_categories', 'int_nice_asthma_review_all': 'synthetic_asthma', 'int_ltc_review_all': 'synthetic_reviews', 'int_structured_medication_review_all': 'synthetic_empty', 'int_falls_discussion_all': 'synthetic_empty', 'int_mrc_dyspnoea_all': 'synthetic_empty', 'int_nyha_classification_all': 'synthetic_empty', 'int_thyroid_function_test_all': 'synthetic_empty', 'int_smi_care_plan_all': 'synthetic_empty'} %}
{% set query = namespace(sql=calculation) %}
{% for model, fixture in replacements.items() %}
    {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_dates AS (
    SELECT CURRENT_DATE()::DATE AS reporting_date
),
synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.reporting_date
    FROM (VALUES (-9311), (-9312)) AS people
    CROSS JOIN synthetic_dates AS dates
),
synthetic_profile AS (
    SELECT
        person_id,
        DATEADD(day, -1, reporting_date)::DATE AS reporting_date,
        FALSE AS has_copd,
        FALSE AS has_heart_failure,
        FALSE AS has_rheumatoid_arthritis,
        FALSE AS has_hypothyroidism,
        FALSE AS has_learning_disability,
        FALSE AS has_dementia,
        FALSE AS has_smi,
        TRUE AS has_cancer,
        NULL::DATE AS latest_new_depression_diagnosis_date,
        NULL::VARCHAR AS latest_frailty_severity,
        DATEADD(day, -60, CURRENT_DATE())::DATE AS latest_cancer_diagnosis_date
    FROM synthetic_population
    WHERE person_id = -9311
),
synthetic_categories AS (
    -- This person has no register or LTC-profile row.
    SELECT
        population.person_id,
        DATEADD(day, -1, reporting_date)::DATE AS reporting_date,
        categories.column1::VARCHAR AS category_code
    FROM synthetic_population AS population
    CROSS JOIN (VALUES ('CHRONIC_PAIN'), ('DIGESTIVE'), ('MENTAL_HEALTH'), ('MUSCULOSKELETAL')) AS categories
    WHERE person_id = -9312
),
synthetic_reviews AS (
    SELECT
        -9311::NUMBER AS person_id,
        DATEADD(day, -30, CURRENT_DATE())::TIMESTAMP_NTZ AS clinical_effective_date,
        'CANCER_CARE_REVIEW'::VARCHAR AS review_type
    UNION ALL
    SELECT -9311::NUMBER, DATEADD(day, 10, CURRENT_DATE())::TIMESTAMP_NTZ, 'CANCER_CARE_REVIEW'
    UNION ALL
    SELECT -9312::NUMBER, DATEADD(day, -30, CURRENT_DATE())::TIMESTAMP_NTZ, 'COPD_REVIEW'
),
synthetic_empty AS (
    SELECT person_id, clinical_effective_date
    FROM synthetic_reviews
    WHERE FALSE
),
synthetic_asthma AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS review_date,
        FALSE AS has_review,
        FALSE AS is_complete_review
    FROM synthetic_reviews
    WHERE FALSE
),
actual AS ({{ query.sql }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = CURRENT_DATE()) <> 2
    OR COUNT_IF(person_id = -9311
        AND first_cancer_care_review_after_diagnosis_date = DATEADD(day, -30, CURRENT_DATE())) <> 1
    OR COUNT_IF(person_id = -9312
        AND latest_copd_review_date = DATEADD(day, -30, CURRENT_DATE())) <> 1
