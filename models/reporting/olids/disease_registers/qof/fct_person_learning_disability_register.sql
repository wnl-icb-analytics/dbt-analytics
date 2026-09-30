-- Pair: macros/qof_registers/calculate_learning_disability_register.sql.
-- This live fact includes future-dated records. Its PIT pair is strict as-of
-- and derives age at the reference date where age is used.

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Learning Disability Register - QOF v50

Business Logic:
- Has learning disability diagnosis (LD_COD)
- NOT excluded: no exclusion code (LDREM_COD) after latest diagnosis
- No age restriction in QOF spec (includes all ages)

QOF Context:
Used for learning disability quality measures including:
- Learning disability care pathway monitoring
- Health equity assessment and improvement
- Special needs service coordination
- Annual health checks (typically age 14+)
*/

WITH learning_disability_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        -- QOF register logic: LD diagnosis without subsequent exclusion
        COALESCE(
            -- Must have an LD diagnosis
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
            -- Must not have been excluded after latest diagnosis
            AND (
                MAX(CASE WHEN is_exclusion_code THEN clinical_effective_date END) IS NULL
                OR MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
                    > MAX(CASE WHEN is_exclusion_code THEN clinical_effective_date END)
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
    GROUP BY person_id
),

register_logic AS (
    SELECT
        ld.person_id,
        age.age,

        -- Clinical dates
        ld.earliest_diagnosis_date,
        ld.latest_diagnosis_date,

        -- Traceability arrays
        ld.all_ld_concept_codes,
        ld.all_ld_concept_displays,

        -- Age flag for downstream use (annual health checks typically 14+)
        COALESCE(age.age >= 14, FALSE) AS is_age_14_or_over,

        -- QOF Register: Active LD diagnosis (no age restriction per QOF v50)
        COALESCE(ld.has_active_ld_diagnosis, FALSE) AS is_on_register

    FROM learning_disability_diagnoses AS ld
    LEFT JOIN {{ ref('dim_person_age') }} AS age ON ld.person_id = age.person_id
    WHERE ld.has_active_ld_diagnosis = TRUE
)

-- Final selection: All patients on the learning disability register
SELECT
    person_id,
    age,
    is_on_register,

    -- Clinical dates
    earliest_diagnosis_date,
    latest_diagnosis_date,

    -- Traceability for audit
    all_ld_concept_codes,
    all_ld_concept_displays,

    -- Criteria flag
    is_age_14_or_over

FROM register_logic
WHERE is_on_register = TRUE
