{% macro calculate_ckd_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_ckd_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates CKD register status at a given reference date.

    Business Logic (QOF v50):
    - Age ≥18 at reference date
    - Has CKD Stage 3-5 diagnosis (CKD_COD)
    - NOT downstaged: no CKD Stage 1-2 code (CKD1AND2_COD) after latest Stage 3-5
    - NOT resolved: no resolved code (CKDRES_COD) after latest Stage 3-5

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH ckd_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_stage_3_5_code,
            is_stage_1_2_code,
            is_resolved_code
        FROM {{ ref('int_ckd_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    ckd_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_stage_1_2_code THEN clinical_effective_date END) AS latest_stage_1_2_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM ckd_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            FLOOR(DATEDIFF('month', birth_date_approx, {{ reference_date_expr }}) / 12) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    ckd_register_logic AS (
        SELECT
            diag.person_id,
            'CKD' AS register_name,
            COALESCE(
                -- Age requirement
                age.age >= 18
                -- Must have a Stage 3-5 diagnosis
                AND diag.latest_diagnosis_date IS NOT NULL
                -- Must not have been downstaged to Stage 1-2 after latest Stage 3-5
                AND (
                    diag.latest_stage_1_2_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_stage_1_2_date
                )
                -- Must not have been resolved after latest Stage 3-5
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_resolved_date
                ),
                FALSE
            ) AS is_on_register
        FROM ckd_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM ckd_register_logic

{% endmacro %}
