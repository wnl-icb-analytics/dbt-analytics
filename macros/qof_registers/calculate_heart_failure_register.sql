{% macro calculate_heart_failure_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_heart_failure_register.sql. Clinical evidence is bounded by the reference date. #}
    {#
    Calculates Heart Failure register status at one or more reference dates.

    Business Logic:
    - Heart failure diagnosis with no resolution on a later date
    - HF3 additionally requires a reduced ejection fraction code
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a heart failure record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date,
        is_on_hfref_register
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    heart_failure_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code,
            diag.is_resolved_code,
            diag.is_reduced_ef_code
        FROM {{ ref('int_heart_failure_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    heart_failure_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
            MIN(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) AS earliest_reduced_ef_diagnosis_date
        FROM heart_failure_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    heart_failure_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Heart Failure' AS register_name,
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                -- HFRES_DAT only considers resolutions after HFLAT_DAT.
                AND (diag.latest_resolved_date IS NULL OR diag.latest_diagnosis_date::DATE >= diag.latest_resolved_date::DATE),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date,
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                AND (diag.latest_resolved_date IS NULL OR diag.latest_diagnosis_date::DATE >= diag.latest_resolved_date::DATE)
                AND diag.earliest_reduced_ef_diagnosis_date IS NOT NULL,
                FALSE
            ) AS is_on_hfref_register
        FROM heart_failure_person_aggregates AS diag
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date,
        is_on_hfref_register
    FROM heart_failure_register_logic

{% endmacro %}
