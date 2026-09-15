{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Clinical non-diabetic hyperglycaemia register.

One row represents one person aged 18 or over with NDH, impaired glucose
tolerance or pre-diabetes evidence and no unresolved diabetes. The QOF v51
gestational-diabetes route is exposed separately through
fct_person_qof_ndh_gdm_register and cannot add a row to this model by itself.
*/

WITH ndh_diagnoses AS (
    SELECT
        person_id,
        MIN(clinical_effective_date) AS earliest_diagnosis_date,
        MAX(clinical_effective_date) AS latest_diagnosis_date,
        MIN(CASE WHEN is_ndh_diagnosis_code
            THEN clinical_effective_date END) AS earliest_ndh_date,
        MAX(CASE WHEN is_ndh_diagnosis_code
            THEN clinical_effective_date END) AS latest_ndh_date,
        MIN(CASE WHEN is_igt_diagnosis_code
            THEN clinical_effective_date END) AS earliest_igt_date,
        MAX(CASE WHEN is_igt_diagnosis_code
            THEN clinical_effective_date END) AS latest_igt_date,
        MIN(CASE WHEN is_pre_diabetes_diagnosis_code
            THEN clinical_effective_date END) AS earliest_prd_date,
        MAX(CASE WHEN is_pre_diabetes_diagnosis_code
            THEN clinical_effective_date END) AS latest_prd_date,
        COUNT(CASE WHEN is_ndh_diagnosis_code THEN 1 END)
            AS total_ndh_episodes,
        COUNT(CASE WHEN is_igt_diagnosis_code THEN 1 END)
            AS total_igt_episodes,
        COUNT(CASE WHEN is_pre_diabetes_diagnosis_code THEN 1 END)
            AS total_prd_episodes,
        MAX(is_ndh_diagnosis_code) AS has_ndh_diagnosis,
        MAX(is_igt_diagnosis_code) AS has_igt_diagnosis,
        MAX(is_pre_diabetes_diagnosis_code) AS has_prd_diagnosis,
        ARRAY_AGG(DISTINCT CASE WHEN is_ndh_diagnosis_code
            THEN concept_code END) AS ndh_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_ndh_diagnosis_code
            THEN concept_display END) AS ndh_diagnosis_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_igt_diagnosis_code
            THEN concept_code END) AS igt_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_igt_diagnosis_code
            THEN concept_display END) AS igt_diagnosis_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_pre_diabetes_diagnosis_code
            THEN concept_code END) AS prd_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_pre_diabetes_diagnosis_code
            THEN concept_display END) AS prd_diagnosis_displays,
        ARRAY_AGG(DISTINCT id::VARCHAR) AS all_ids
    FROM {{ ref('int_ndh_diagnoses_all') }}
    GROUP BY person_id
),

diabetes_status AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_general_diabetes_code
            THEN clinical_effective_date END)
            AS earliest_diabetes_diagnosis_date,
        MAX(CASE WHEN is_general_diabetes_code
            THEN clinical_effective_date END)
            AS latest_diabetes_diagnosis_date,
        MAX(CASE WHEN is_diabetes_resolved_code
            THEN clinical_effective_date END)
            AS latest_diabetes_resolved_date
    FROM {{ ref('int_diabetes_diagnoses_all') }}
    GROUP BY person_id
),

register_members AS (
    SELECT
        diagnosis.*,
        age.age,
        diabetes.earliest_diabetes_diagnosis_date IS NOT NULL
            AS has_diabetes_diagnosis,
        COALESCE(
            diabetes.latest_diabetes_resolved_date
                > diabetes.latest_diabetes_diagnosis_date,
            FALSE
        ) AS is_diabetes_resolved,
        diabetes.earliest_diabetes_diagnosis_date,
        diabetes.latest_diabetes_resolved_date
    FROM ndh_diagnoses AS diagnosis
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON diagnosis.person_id = age.person_id
    LEFT JOIN diabetes_status AS diabetes
        ON diagnosis.person_id = diabetes.person_id
    WHERE age.age >= 18
        AND (
            diabetes.earliest_diabetes_diagnosis_date IS NULL
            OR diabetes.latest_diabetes_resolved_date
                > diabetes.latest_diabetes_diagnosis_date
        )
)

SELECT
    membership.person_id,
    TRUE AS is_on_register,
    'NDH - eligible for diabetes prevention' AS ndh_status,
    membership.earliest_diagnosis_date,
    membership.latest_diagnosis_date,
    membership.earliest_ndh_date,
    membership.latest_ndh_date,
    membership.earliest_igt_date,
    membership.latest_igt_date,
    membership.earliest_prd_date,
    membership.latest_prd_date,
    membership.age - DATEDIFF(
        YEAR,
        membership.earliest_diagnosis_date,
        CURRENT_DATE()
    ) AS age_at_first_ndh_diagnosis,
    membership.total_ndh_episodes,
    membership.total_igt_episodes,
    membership.total_prd_episodes,
    membership.has_ndh_diagnosis,
    membership.has_igt_diagnosis,
    membership.has_prd_diagnosis,
    TRUE AS has_ndh_route,
    membership.has_diabetes_diagnosis,
    membership.is_diabetes_resolved,
    membership.earliest_diabetes_diagnosis_date,
    membership.latest_diabetes_resolved_date,
    membership.ndh_diagnosis_codes,
    membership.ndh_diagnosis_displays,
    membership.igt_diagnosis_codes,
    membership.igt_diagnosis_displays,
    membership.prd_diagnosis_codes,
    membership.prd_diagnosis_displays,
    membership.all_ids
FROM register_members AS membership
ORDER BY membership.earliest_diagnosis_date DESC, membership.person_id
