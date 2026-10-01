{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        persist_docs={"relation": true, "columns": true})
}}

/*
Thalassaemia Register

Thalassaemia is an inherited blood disorder causing anaemia; severe forms need lifelong
blood transfusions and iron-removal treatment. Diagnosis-only register (lifelong, no
resolution codes, non-QOF, no age limit).

Business Logic:
- Any THAL_COD diagnosis recorded = on register
*/

WITH thalassaemia_diagnoses AS (
    SELECT
        person_id,

        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        COALESCE(MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL, FALSE) AS has_active_diagnosis,

        COUNT(CASE WHEN is_diagnosis_code THEN 1 END) AS total_diagnoses,

        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END)
            AS all_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END)
            AS all_concept_displays

    FROM {{ ref('int_thalassaemia_diagnoses_all') }}
    GROUP BY person_id
)

SELECT
    thal.person_id,
    age.age,
    TRUE AS is_on_register,
    thal.earliest_diagnosis_date,
    thal.latest_diagnosis_date,
    thal.total_diagnoses,
    thal.all_concept_codes,
    thal.all_concept_displays

FROM thalassaemia_diagnoses AS thal
INNER JOIN {{ ref('dim_person') }} AS p
    ON thal.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON thal.person_id = age.person_id
WHERE thal.has_active_diagnosis = TRUE
