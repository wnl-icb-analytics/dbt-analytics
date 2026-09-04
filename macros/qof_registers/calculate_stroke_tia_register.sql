{% macro calculate_stroke_tia_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_stroke_tia_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Stroke/TIA register status at a given reference date.

    Business Logic:
    - Presence of stroke or TIA diagnosis = on register (lifelong condition, no resolution)
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH stroke_tia_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_stroke_tia_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
          AND is_diagnosis_code = TRUE
    ),

    stroke_tia_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date
        FROM stroke_tia_diagnoses_filtered
        GROUP BY person_id
    ),

    stroke_tia_register_logic AS (
        SELECT
            diag.person_id,
            'Stroke/TIA' AS register_name,
            COALESCE(diag.earliest_diagnosis_date IS NOT NULL, FALSE) AS is_on_register
        FROM stroke_tia_person_aggregates diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM stroke_tia_register_logic

{% endmacro %}
