{% macro calculate_nafld_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_nafld_register.sql. This macro is strict as-of; the live fact includes future-dated records and filters to current active patients. #}
    {#
    Calculates MASLD / NAFLD register status at one or more reference dates.

    Business Logic:
    - MASLD / NAFLD diagnosis (MASLD_DX_CODES) known by the reference date
    - No resolution codes
    - No age restriction
    Current active-patient filtering is omitted; monthly models apply registration
    and living status at each month-end.

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with nafld records known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    nafld_person_aggregates AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            MIN(CASE WHEN diag.is_diagnosis_code THEN diag.clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN diag.is_diagnosis_code THEN diag.clinical_effective_date END) AS latest_diagnosis_date
        FROM {{ ref('int_nafld_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, diag.person_id
    )

    SELECT
        reference_date,
        person_id,
        'MASLD / NAFLD' AS register_name,
        earliest_diagnosis_date IS NOT NULL AS is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM nafld_person_aggregates

{% endmacro %}
