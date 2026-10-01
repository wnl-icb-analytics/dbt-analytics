{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        persist_docs={"relation": true, "columns": true})
}}

/*
Sickle Cell Disease Register

Sickle cell disease is an inherited blood disorder causing painful crises, anaemia and
raised stroke/infection risk. Diagnosis-only register (lifelong, no resolution codes,
non-QOF, no age limit).

Business Logic:
- Any SICKLE_COD diagnosis recorded = on register
*/

WITH sickle_cell_diagnoses AS (
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

    FROM {{ ref('int_sickle_cell_diagnoses_all') }}
    GROUP BY person_id
)

SELECT
    scd.person_id,
    age.age,
    TRUE AS is_on_register,
    scd.earliest_diagnosis_date,
    scd.latest_diagnosis_date,
    scd.total_diagnoses,
    scd.all_concept_codes,
    scd.all_concept_displays

FROM sickle_cell_diagnoses AS scd
INNER JOIN {{ ref('dim_person') }} AS p
    ON scd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON scd.person_id = age.person_id
WHERE scd.has_active_diagnosis = TRUE
