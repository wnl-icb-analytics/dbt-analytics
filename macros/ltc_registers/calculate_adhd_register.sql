{% macro calculate_adhd_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_adhd_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates ADHD register status at one or more reference dates.

    Business Logic:
    - ADHD diagnosis (ADHD_COD) known by the reference date
    - Unresolved: latest diagnosis later than the latest remission (ADHDREM_COD)
    - No age restriction

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with adhd records known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    adhd_person_aggregates AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            MIN(CASE WHEN diag.is_diagnosis_code THEN diag.clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN diag.is_diagnosis_code THEN diag.clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN diag.is_resolved_code THEN diag.clinical_effective_date END) AS latest_resolved_date
        FROM {{ ref('int_adhd_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, diag.person_id
    )

    SELECT
        reference_date,
        person_id,
        'ADHD' AS register_name,
        COALESCE(
            latest_diagnosis_date IS NOT NULL
            AND (latest_resolved_date IS NULL OR latest_diagnosis_date > latest_resolved_date),
            FALSE
        ) AS is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date
    FROM adhd_person_aggregates

{% endmacro %}
