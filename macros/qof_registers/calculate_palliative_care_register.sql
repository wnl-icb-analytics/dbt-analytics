{% macro calculate_palliative_care_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_palliative_care_register.sql. Clinical evidence is bounded by the reference date. #}
    {#
    Calculates Palliative Care register status at one or more reference dates.

    Business Logic:
    - Palliative care diagnosis on/after 1 April 2008
    - No "no longer indicated" code on a later calendar date than latest care
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a palliative care record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date (latest no-longer-indicated code)
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    palliative_care_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_palliative_care_code,
            diag.is_palliative_care_not_indicated_code
        FROM {{ ref('int_palliative_care_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    palliative_care_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_palliative_care_code AND clinical_effective_date::DATE >= '2008-04-01' THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_palliative_care_code AND clinical_effective_date::DATE >= '2008-04-01' THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_palliative_care_not_indicated_code THEN clinical_effective_date END) AS latest_no_longer_indicated_date
        FROM palliative_care_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    palliative_care_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Palliative Care' AS register_name,
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                AND (diag.latest_no_longer_indicated_date IS NULL OR diag.latest_no_longer_indicated_date::DATE <= diag.latest_diagnosis_date::DATE),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_no_longer_indicated_date AS latest_resolved_date
        FROM palliative_care_person_aggregates AS diag
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date
    FROM palliative_care_register_logic

{% endmacro %}
