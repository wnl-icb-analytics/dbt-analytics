{% macro calculate_ckd_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_ckd_register.sql. This macro is strict as-of; the live fact uses evidence dated on or before today. #}
    {#
    Calculates CKD register status at one or more reference dates.

    Business Logic (QOF v51):
    - Age ≥18 at reference date
    - Has CKD Stage 3-5 diagnosis (CKD_COD)
    - NOT downstaged: no CKD Stage 1-2 code (CKD1AND2_COD) after latest Stage 3-5
    - NOT resolved: no resolved code (CKDRES_COD) after latest Stage 3-5
    - A same-day Stage 1-2 or resolved code does not remove the diagnosis;
      comparisons use the date, not the time

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date, latest_stage_1_2_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    ckd_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_stage_3_5_code,
            is_stage_1_2_code,
            is_resolved_code
        FROM {{ ref('int_ckd_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    ckd_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_stage_3_5_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_stage_1_2_code THEN clinical_effective_date END) AS latest_stage_1_2_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM ckd_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    age_at_reference AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            FLOOR(DATEDIFF('month', birth.birth_date_approx, diag.reference_date) / 12) AS age
        FROM ckd_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    ckd_register_logic AS (
        SELECT
            diag.reference_date,
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
                    OR diag.latest_diagnosis_date::DATE >= diag.latest_stage_1_2_date::DATE
                )
                -- Must not have been resolved after latest Stage 3-5
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date::DATE >= diag.latest_resolved_date::DATE
                ),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date,
            diag.latest_stage_1_2_date
        FROM ckd_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date,
        latest_stage_1_2_date
    FROM ckd_register_logic

{% endmacro %}
