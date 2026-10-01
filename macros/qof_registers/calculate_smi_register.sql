{% macro calculate_smi_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_smi_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates SMI (Severe Mental Illness) register status at one or more reference dates.

    Per QOF v51 MH1_REG:
    - Ever diagnosed with MH_COD (remission codes do not qualify on their own)
    - Lithium therapy is not a register membership route
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a diagnosis known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_remission_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    smi_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code,
            diag.is_resolved_code
        FROM {{ ref('int_smi_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    smi_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_remission_date
        FROM smi_diagnoses_filtered
        GROUP BY reference_date, person_id
    )

    SELECT
        reference_date,
        person_id,
        'SMI' AS register_name,
        TRUE AS is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_remission_date
    FROM smi_person_aggregates
    WHERE latest_diagnosis_date IS NOT NULL

{% endmacro %}
