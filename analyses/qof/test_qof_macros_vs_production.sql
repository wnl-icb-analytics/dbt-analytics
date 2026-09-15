{{
    config(
        tags=['qof', 'test', 'validation']
    )
}}

/*
Test Analysis: QOF Register Views vs Production Models

Compares counts from QOF register pit views against production models.
This validates that the register view logic matches the production implementation.

Usage:
    dbt compile --select test_qof_macros_vs_production

Expected Result:
    macro_count should equal production_count for all registers
*/

WITH reference_date_cte AS (
    SELECT CURRENT_DATE() AS reference_date
),

-- Get practice information for all persons (not just active)
-- Production models include all people on register regardless of current registration status
person_practices AS (
    SELECT
        person_id,
        practice_code,
        practice_name
    FROM {{ ref('dim_person_demographics') }}
),

-- Query pit register views for each condition
macro_diabetes AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_diabetes_register') }} v ON ar.person_id = v.person_id
),

macro_asthma AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_asthma_register') }} v ON ar.person_id = v.person_id
),

macro_chd AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_chd_register') }} v ON ar.person_id = v.person_id
),

macro_hypertension AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_hypertension_register') }} v ON ar.person_id = v.person_id
),

macro_copd AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_copd_register') }} v ON ar.person_id = v.person_id
),

macro_ckd AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_ckd_register') }} v ON ar.person_id = v.person_id
),

macro_atrial_fibrillation AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_atrial_fibrillation_register') }} v ON ar.person_id = v.person_id
),

macro_heart_failure AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_heart_failure_register') }} v ON ar.person_id = v.person_id
),

macro_stroke_tia AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_stroke_tia_register') }} v ON ar.person_id = v.person_id
),

macro_pad AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_pad_register') }} v ON ar.person_id = v.person_id
),

macro_dementia AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_dementia_register') }} v ON ar.person_id = v.person_id
),

macro_depression AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_depression_register') }} v ON ar.person_id = v.person_id
),

macro_smi AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_smi_register') }} v ON ar.person_id = v.person_id
),

macro_epilepsy AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_epilepsy_register') }} v ON ar.person_id = v.person_id
),

macro_cancer AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_cancer_register') }} v ON ar.person_id = v.person_id
),

macro_palliative_care AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_palliative_care_register') }} v ON ar.person_id = v.person_id
),

macro_learning_disability AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_learning_disability_register') }} v ON ar.person_id = v.person_id
),

macro_osteoporosis AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_osteoporosis_register') }} v ON ar.person_id = v.person_id
),

macro_rheumatoid_arthritis AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_rheumatoid_arthritis_register') }} v ON ar.person_id = v.person_id
),

macro_ndh AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_qof_ndh_gdm_register') }} v ON ar.person_id = v.person_id
),

macro_obesity AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_obesity_register') }} v ON ar.person_id = v.person_id
),

-- Aggregate macro counts
macro_counts AS (
    SELECT 'Diabetes' AS register_name, SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) AS macro_count FROM macro_diabetes
    UNION ALL SELECT 'Asthma', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_asthma
    UNION ALL SELECT 'CHD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_chd
    UNION ALL SELECT 'Hypertension', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_hypertension
    UNION ALL SELECT 'COPD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_copd
    UNION ALL SELECT 'CKD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_ckd
    UNION ALL SELECT 'Atrial Fibrillation', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_atrial_fibrillation
    UNION ALL SELECT 'Heart Failure', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_heart_failure
    UNION ALL SELECT 'Stroke/TIA', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_stroke_tia
    UNION ALL SELECT 'PAD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_pad
    UNION ALL SELECT 'Dementia', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_dementia
    UNION ALL SELECT 'Depression', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_depression
    UNION ALL SELECT 'SMI', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_smi
    UNION ALL SELECT 'Epilepsy', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_epilepsy
    UNION ALL SELECT 'Cancer', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_cancer
    UNION ALL SELECT 'Palliative Care', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_palliative_care
    UNION ALL SELECT 'Learning Disability', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_learning_disability
    UNION ALL SELECT 'Osteoporosis', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_osteoporosis
    UNION ALL SELECT 'Rheumatoid Arthritis', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_rheumatoid_arthritis
    UNION ALL SELECT 'NDH', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_ndh
    UNION ALL SELECT 'Obesity', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM macro_obesity
),

-- Get production counts
production_counts AS (
    SELECT 'Diabetes' AS register_name, COUNT(*) AS production_count FROM {{ ref('fct_person_diabetes_register') }}
    UNION ALL SELECT 'Asthma', COUNT(*) FROM {{ ref('fct_person_asthma_register') }}
    UNION ALL SELECT 'CHD', COUNT(*) FROM {{ ref('fct_person_chd_register') }}
    UNION ALL SELECT 'Hypertension', COUNT(*) FROM {{ ref('fct_person_hypertension_register') }}
    UNION ALL SELECT 'COPD', COUNT(*) FROM {{ ref('fct_person_copd_register') }}
    UNION ALL SELECT 'CKD', COUNT(*) FROM {{ ref('fct_person_ckd_register') }}
    UNION ALL SELECT 'Atrial Fibrillation', COUNT(*) FROM {{ ref('fct_person_atrial_fibrillation_register') }}
    UNION ALL SELECT 'Heart Failure', COUNT(*) FROM {{ ref('fct_person_heart_failure_register') }}
    UNION ALL SELECT 'Stroke/TIA', COUNT(*) FROM {{ ref('fct_person_stroke_tia_register') }}
    UNION ALL SELECT 'PAD', COUNT(*) FROM {{ ref('fct_person_pad_register') }}
    UNION ALL SELECT 'Dementia', COUNT(*) FROM {{ ref('fct_person_dementia_register') }}
    UNION ALL SELECT 'Depression', COUNT(*) FROM {{ ref('fct_person_depression_register') }}
    UNION ALL SELECT 'SMI', COUNT(*) FROM {{ ref('fct_person_smi_register') }}
    UNION ALL SELECT 'Epilepsy', COUNT(*) FROM {{ ref('fct_person_epilepsy_register') }}
    UNION ALL SELECT 'Cancer', COUNT(*) FROM {{ ref('fct_person_cancer_register') }}
    UNION ALL SELECT 'Palliative Care', COUNT(*) FROM {{ ref('fct_person_palliative_care_register') }}
    UNION ALL SELECT 'Learning Disability', COUNT(*) FROM {{ ref('fct_person_learning_disability_register') }}
    UNION ALL SELECT 'Osteoporosis', COUNT(*) FROM {{ ref('fct_person_osteoporosis_register') }}
    UNION ALL SELECT 'Rheumatoid Arthritis', COUNT(*) FROM {{ ref('fct_person_rheumatoid_arthritis_register') }}
    UNION ALL SELECT 'NDH', COUNT(*) FROM {{ ref('fct_person_qof_ndh_gdm_register') }}
    UNION ALL SELECT 'Obesity', COUNT(*) FROM {{ ref('fct_person_obesity_register') }}
)

-- Compare counts (2% tolerance threshold)
SELECT
    mc.register_name,
    mc.macro_count,
    pc.production_count,
    mc.macro_count - pc.production_count AS difference,
    CASE
        WHEN ABS(100.0 * mc.macro_count / NULLIF(pc.production_count, 0) - 100) <= 2 THEN '✓ PASS'
        ELSE '✗ FAIL'
    END AS test_result,
    ROUND(100.0 * mc.macro_count / NULLIF(pc.production_count, 0), 2) AS percent_match
FROM macro_counts mc
FULL OUTER JOIN production_counts pc ON mc.register_name = pc.register_name
ORDER BY
    CASE WHEN ABS(100.0 * mc.macro_count / NULLIF(pc.production_count, 0) - 100) <= 2 THEN 1 ELSE 0 END,
    mc.register_name
