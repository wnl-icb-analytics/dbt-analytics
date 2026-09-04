{% macro calculate_hypertension_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_hypertension_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Hypertension register status at a given reference date.

    QOF v50 HYP_REG (CQRS 001):
    - HYPLAT_DAT ≠ Null AND HYPRES_DAT = Null: an unresolved hypertension diagnosis
      (latest diagnosis after the latest resolution). NO age restriction - the register
      is all-ages. PAT_AGE applies only to the BP-target indicator denominators, not here.

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH hypertension_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_hypertension_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    hypertension_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM hypertension_diagnoses_filtered
        GROUP BY person_id
    ),

    hypertension_register_logic AS (
        SELECT
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
            ) AS is_on_register
        FROM hypertension_person_aggregates diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM hypertension_register_logic

{% endmacro %}
