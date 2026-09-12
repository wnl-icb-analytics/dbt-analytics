-- Pair: macros/qof_registers/calculate_heart_failure_register.sql.
-- This live fact includes future-dated records; its PIT pair is strict as-of.

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Heart Failure Register (QOF Pattern 4: Type Classification Register)
-- Business Logic: HF1 is active HF; HF3 also requires a reduced ejection fraction code
-- Multiple registers: General HF1 and reduced-ejection-fraction-only HF3

WITH heart_failure_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE
                WHEN
                    is_diagnosis_code
                    THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN
                    is_diagnosis_code
                    THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,
        MAX(
            CASE
                WHEN is_resolved_code THEN clinical_effective_date
            END
        ) AS latest_resolved_date,

        -- Reduced ejection fraction dates
        MIN(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END)
            AS earliest_reduced_ef_diagnosis_date,
        MAX(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END)
            AS latest_reduced_ef_diagnosis_date,

        -- QOF register logic: active HF diagnosis required
        COALESCE(MAX(
            CASE
                WHEN
                    is_diagnosis_code
                    THEN clinical_effective_date
            END
        ) IS NOT NULL
        AND (
            MAX(
                CASE
                    WHEN
                        is_resolved_code
                        THEN clinical_effective_date
                END
            ) IS NULL
            OR MAX(
                CASE
                    WHEN
                        is_diagnosis_code
                        THEN clinical_effective_date
                END
            )
            > MAX(
                CASE
                    WHEN
                        is_resolved_code
                        THEN clinical_effective_date
                END
            )
        ), FALSE) AS has_active_hf_diagnosis,

        COALESCE(MAX(
            CASE WHEN is_reduced_ef_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_reduced_ef_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_code
            END
        ) AS all_hf_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_hf_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_resolved_code THEN concept_code
            END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_heart_failure_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        diag.person_id,
        age.age,

        -- No age restrictions for HF register
        TRUE AS meets_age_criteria,

        -- Diagnosis component
        diag.earliest_diagnosis_date,

        -- General HF register inclusion
        diag.latest_diagnosis_date,

        -- HF3 register inclusion (subset of general HF register)
        diag.latest_resolved_date,

        -- Clinical dates
        diag.earliest_reduced_ef_diagnosis_date,
        diag.latest_reduced_ef_diagnosis_date,
        diag.has_reduced_ef_diagnosis,
        diag.all_hf_concept_codes,

        -- Subtype flags
        diag.all_hf_concept_displays,
        diag.all_resolved_concept_codes,

        -- Traceability
        COALESCE(diag.has_active_hf_diagnosis, FALSE) AS has_active_diagnosis,
        COALESCE(diag.has_active_hf_diagnosis = TRUE, FALSE) AS is_on_register,
        COALESCE(
            diag.has_active_hf_diagnosis = TRUE
            AND diag.has_reduced_ef_diagnosis = TRUE,
            FALSE
        ) AS is_on_hfref_register
    FROM heart_failure_diagnoses AS diag
    LEFT JOIN {{ ref('dim_person_age') }} AS age ON diag.person_id = age.person_id
)

-- Final selection: Only individuals on heart failure register
SELECT
    person_id,
    age,
    is_on_register,
    is_on_hfref_register,

    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,

    -- Reduced ejection fraction dates
    earliest_reduced_ef_diagnosis_date,
    latest_reduced_ef_diagnosis_date,

    -- Subtype classification flags
    has_reduced_ef_diagnosis,

    -- Traceability for audit
    all_hf_concept_codes,
    all_hf_concept_displays,
    all_resolved_concept_codes,

    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_register = TRUE
