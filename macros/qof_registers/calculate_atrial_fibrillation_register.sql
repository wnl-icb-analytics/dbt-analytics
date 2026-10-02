{% macro calculate_atrial_fibrillation_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_atrial_fibrillation_register.sql. This macro is strict as-of; the live fact uses evidence dated on or before today. #}
    {#
    Calculates Atrial Fibrillation register status at one or more reference dates.

    Business Logic:
    - Active AF diagnosis (latest diagnosis date after the latest resolution date OR no resolution)
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with an AF record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    atrial_fibrillation_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code,
            diag.is_resolved_code
        FROM {{ ref('int_atrial_fibrillation_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    atrial_fibrillation_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM atrial_fibrillation_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    atrial_fibrillation_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Atrial Fibrillation' AS register_name,
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                AND (diag.latest_resolved_date IS NULL OR diag.latest_diagnosis_date::DATE > diag.latest_resolved_date::DATE),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date
        FROM atrial_fibrillation_person_aggregates AS diag
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date
    FROM atrial_fibrillation_register_logic

{% endmacro %}
