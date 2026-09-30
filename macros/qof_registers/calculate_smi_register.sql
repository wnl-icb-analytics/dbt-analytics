{% macro calculate_smi_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_smi_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates SMI (Severe Mental Illness) register status at a given reference date.

    Per QOF v51 MH1_REG:
    - Ever diagnosed with MH_COD (remission codes do not qualify on their own)
    - Lithium therapy is not a register membership route
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH smi_diagnoses_filtered AS (
        SELECT DISTINCT person_id
        FROM {{ ref('int_smi_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
          AND is_diagnosis_code = TRUE
    ),

    smi_register_logic AS (
        SELECT
            diag.person_id,
            'SMI' AS register_name,
            TRUE AS is_on_register
        FROM smi_diagnoses_filtered diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM smi_register_logic

{% endmacro %}
