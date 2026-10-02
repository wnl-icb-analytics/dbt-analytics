{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Learning Disability Register (Under 14)

Business Logic:
- Has learning disability diagnosis (LD_COD)
- NOT excluded: no exclusion code (LDREM_COD) on or after latest diagnosis
- Age < 14 years (specialist settings cohort)

Clinical Context:
Children under 14 with learning disabilities typically have their care
managed in specialist paediatric settings rather than general practice.
This model supports identification of this cohort.
*/

WITH learning_disability_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        -- Register logic: no LD exclusion on or after latest diagnosis
        COALESCE(
            -- Must have an LD diagnosis
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
            -- Must not have been excluded on or after latest diagnosis
            AND (
                MAX(CASE WHEN is_exclusion_code THEN clinical_effective_date END) IS NULL
                OR MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)::DATE
                    > MAX(CASE WHEN is_exclusion_code THEN clinical_effective_date END)::DATE
            ),
            FALSE
        ) AS has_active_ld_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_ld_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_ld_concept_displays

    FROM {{ ref('int_learning_disability_diagnoses_all') }}
    WHERE CAST(clinical_effective_date AS DATE) <= CURRENT_DATE()
    GROUP BY person_id
)

SELECT
    ld.person_id,
    age.age,
    ld.has_active_ld_diagnosis AS is_on_register,
    ld.earliest_diagnosis_date,
    ld.latest_diagnosis_date,
    ld.all_ld_concept_codes,
    ld.all_ld_concept_displays
FROM learning_disability_diagnoses AS ld
INNER JOIN {{ ref('dim_person_age') }} AS age ON ld.person_id = age.person_id
WHERE ld.has_active_ld_diagnosis = TRUE
  AND age.age < 14
