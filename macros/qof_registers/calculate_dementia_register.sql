{% macro calculate_dementia_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_dementia_register.sql. This macro is strict as-of; the live fact uses evidence dated on or before today. #}
    {#
    Calculates Dementia register status at one or more reference dates.

    Business Logic:
    - Presence of dementia diagnosis = on register (lifelong condition, no resolution)
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a diagnosis known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    dementia_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code
        FROM {{ ref('int_dementia_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
        WHERE diag.is_diagnosis_code = TRUE
    ),

    dementia_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date,
            MAX(clinical_effective_date) AS latest_diagnosis_date
        FROM dementia_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    dementia_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Dementia' AS register_name,
            COALESCE(diag.earliest_diagnosis_date IS NOT NULL, FALSE) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date
        FROM dementia_person_aggregates AS diag
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM dementia_register_logic

{% endmacro %}
