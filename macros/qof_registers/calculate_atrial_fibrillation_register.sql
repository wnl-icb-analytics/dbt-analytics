{% macro calculate_atrial_fibrillation_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_atrial_fibrillation_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Atrial Fibrillation register status at a given reference date.

    Business Logic:
    - Active AF diagnosis (latest diagnosis > latest resolution OR no resolution)
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH atrial_fibrillation_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_atrial_fibrillation_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    atrial_fibrillation_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM atrial_fibrillation_diagnoses_filtered
        GROUP BY person_id
    ),

    atrial_fibrillation_register_logic AS (
        SELECT
            diag.person_id,
            'Atrial Fibrillation' AS register_name,
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                AND (diag.latest_resolved_date IS NULL OR diag.latest_diagnosis_date > diag.latest_resolved_date),
                FALSE
            ) AS is_on_register
        FROM atrial_fibrillation_person_aggregates diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM atrial_fibrillation_register_logic

{% endmacro %}
