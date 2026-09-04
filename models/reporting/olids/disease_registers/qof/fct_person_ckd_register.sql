-- Pair: macros/qof_registers/calculate_ckd_register.sql.
-- This live fact includes future-dated records; its PIT pair is strict as-of.

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Chronic Kidney Disease (CKD) Register - QOF v50

Business Logic:
- Age ≥18 years
- Has CKD Stage 3-5 diagnosis (CKD_COD)
- NOT downstaged: no CKD Stage 1-2 code (CKD1AND2_COD) after latest Stage 3-5
- NOT resolved: no resolved code (CKDRES_COD) after latest Stage 3-5

Lab data available separately in intermediate tables for clinical monitoring.
*/

WITH ckd_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,
        MAX(CASE WHEN is_stage_1_2_code THEN clinical_effective_date END)
            AS latest_stage_1_2_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            AS latest_resolved_date,

        -- QOF register logic: active Stage 3-5 diagnosis required
        -- Excluded if downstaged to Stage 1-2 OR resolved after latest Stage 3-5
        COALESCE(
            -- Must have a Stage 3-5 diagnosis
            MAX(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END) IS NOT NULL
            -- Must not have been downstaged to Stage 1-2 after latest Stage 3-5
            AND (
                MAX(CASE WHEN is_stage_1_2_code THEN clinical_effective_date END) IS NULL
                OR MAX(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END)
                    > MAX(CASE WHEN is_stage_1_2_code THEN clinical_effective_date END)
            )
            -- Must not have been resolved after latest Stage 3-5
            AND (
                MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NULL
                OR MAX(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END)
                    > MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            ),
            FALSE
        ) AS has_active_ckd_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_stage_3_5_code THEN concept_code END
        ) AS all_ckd_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_stage_1_2_code THEN concept_code END
        ) AS all_stage_1_2_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_ckd_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        diag.person_id,
        age.age,

        -- Clinical dates
        diag.earliest_diagnosis_date,
        diag.latest_diagnosis_date,
        diag.latest_stage_1_2_date,
        diag.latest_resolved_date,

        -- Traceability arrays
        diag.all_ckd_concept_codes,
        diag.all_stage_1_2_concept_codes,
        diag.all_resolved_concept_codes,

        -- Criteria flags for transparency
        COALESCE(age.age >= 18, FALSE) AS meets_age_criteria,
        COALESCE(diag.has_active_ckd_diagnosis, FALSE) AS has_active_diagnosis,

        -- Downstaging flag: TRUE if patient was downstaged to Stage 1-2 after Stage 3-5
        COALESCE(
            diag.latest_stage_1_2_date IS NOT NULL
            AND diag.latest_stage_1_2_date > diag.latest_diagnosis_date,
            FALSE
        ) AS was_downstaged,

        -- Final register inclusion: Age + Active diagnosis (not downstaged, not resolved)
        COALESCE(
            age.age >= 18
            AND diag.has_active_ckd_diagnosis = TRUE,
            FALSE
        ) AS is_on_register

    FROM ckd_diagnoses AS diag
    INNER JOIN {{ ref('dim_person_age') }} AS age ON diag.person_id = age.person_id
)

-- Final selection: Only individuals with active CKD Stage 3-5 diagnosis
SELECT
    person_id,
    age,
    is_on_register,

    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_stage_1_2_date,
    latest_resolved_date,

    -- Traceability for audit
    all_ckd_concept_codes,
    all_stage_1_2_concept_codes,
    all_resolved_concept_codes,

    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis,
    was_downstaged

FROM register_logic
WHERE is_on_register = TRUE
