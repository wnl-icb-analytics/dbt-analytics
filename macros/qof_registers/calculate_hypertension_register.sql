{% macro calculate_hypertension_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_hypertension_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Hypertension register status at one or more reference dates.

    QOF v50 HYP_REG (CQRS 001):
    - HYPLAT_DAT ≠ Null AND HYPRES_DAT = Null: an unresolved hypertension diagnosis
      (latest diagnosis after the latest resolution). NO age restriction - the register
      is all-ages. PAT_AGE applies only to the BP-target indicator denominators, not here.

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a hypertension record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    hypertension_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code,
            diag.is_resolved_code
        FROM {{ ref('int_hypertension_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    hypertension_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM hypertension_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    hypertension_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Hypertension' AS register_name,
            -- Unresolved hypertension diagnosis (HYPLAT_DAT ≠ Null AND HYPRES_DAT = Null).
            -- HYPRES_DAT is the latest resolution STRICTLY after the latest diagnosis, so a
            -- resolution on/before the latest diagnosis does not resolve the register: use >=.
            -- Explicit NULL check (no '1900-01-01' sentinel, which collided with null-dated
            -- diagnosis codes coalesced to 1900). No age restriction per QOF v50.
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date >= diag.latest_resolved_date
                ),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date
        FROM hypertension_person_aggregates AS diag
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date
    FROM hypertension_register_logic

{% endmacro %}
