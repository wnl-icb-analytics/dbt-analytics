{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = calculate_nice_review_evidence('by_month') %}
{% set calculation = calculation | replace(nice_asthma_diagnosis_population('by_month') | string, 'SELECT person_id, reporting_date FROM synthetic_population WHERE FALSE') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set replacements = {'int_nice_ltc_population_by_month': 'synthetic_profile', 'int_nice_multimorbidity_categories_by_month': 'synthetic_categories', 'int_nice_asthma_review_all': 'synthetic_asthma', 'int_ltc_review_all': 'synthetic_reviews', 'int_structured_medication_review_all': 'synthetic_empty', 'int_falls_discussion_all': 'synthetic_empty', 'int_mrc_dyspnoea_all': 'synthetic_empty', 'int_nyha_classification_all': 'synthetic_empty', 'int_thyroid_function_test_all': 'synthetic_empty', 'int_smi_care_plan_all': 'synthetic_empty'} %}
{% set query = namespace(sql=calculation) %}
{% for model, fixture in replacements.items() %}
    {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_dates AS (
    SELECT column1::DATE AS reporting_date
    FROM VALUES ('2026-08-31'), ('2026-09-30')
),
synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.reporting_date
    FROM (VALUES (-9311), (-9312), (-9313)) AS people
    CROSS JOIN synthetic_dates AS dates
),
synthetic_profile AS (
    SELECT
        person_id,
        reporting_date,
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
        '2026-08-01'::DATE AS latest_cancer_diagnosis_date
    FROM synthetic_population
    WHERE person_id IN (-9311, -9313)
),
synthetic_categories AS (
    -- This person has no register or LTC-profile row.
    SELECT
        population.person_id,
        reporting_date,
        categories.column1::VARCHAR AS category_code
    FROM synthetic_population AS population
    CROSS JOIN (VALUES ('CHRONIC_PAIN'), ('DIGESTIVE'), ('MENTAL_HEALTH'), ('MUSCULOSKELETAL')) AS categories
    WHERE person_id = -9312
),
synthetic_reviews AS (
    SELECT
        -9311::NUMBER AS person_id,
        '2026-09-01'::TIMESTAMP_NTZ AS clinical_effective_date,
        'CANCER_CARE_REVIEW'::VARCHAR AS review_type
    UNION ALL
    SELECT -9311::NUMBER, '2026-10-10'::TIMESTAMP_NTZ, 'CANCER_CARE_REVIEW'
    UNION ALL
    -- A review before diagnosis cannot satisfy cancer follow-up.
    SELECT -9311::NUMBER, '2026-07-31'::TIMESTAMP_NTZ, 'CANCER_CARE_REVIEW'
    UNION ALL
    SELECT -9313::NUMBER, '2026-08-01'::TIMESTAMP_NTZ, 'CANCER_CARE_REVIEW'
    UNION ALL
    SELECT -9312::NUMBER, '2026-09-01'::TIMESTAMP_NTZ, 'COPD_REVIEW'
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
HAVING COUNT(*) <> 6
    OR COUNT_IF(reporting_date = '2026-08-31'
        AND first_cancer_care_review_after_diagnosis_date IS NULL
        AND latest_copd_review_date IS NULL) <> 2
    OR COUNT_IF(person_id = -9311 AND reporting_date = '2026-09-30'
        AND first_cancer_care_review_after_diagnosis_date = '2026-09-01') <> 1
    OR COUNT_IF(person_id = -9312 AND reporting_date = '2026-09-30'
        AND latest_copd_review_date = '2026-09-01') <> 1
    OR COUNT_IF(person_id = -9313
        AND first_cancer_care_review_after_diagnosis_date = '2026-08-01') <> 2
