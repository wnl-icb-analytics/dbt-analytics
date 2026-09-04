{% macro calculate_cancer_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_cancer_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Cancer register status at a given reference date.

    Business Logic:
    - Cancer diagnosis on/after 1 April 2003 (QOF start date)
    - Lifelong condition, no resolution
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH cancer_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_cancer_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
          AND clinical_effective_date >= '2003-04-01'
          AND is_diagnosis_code = TRUE
          AND is_first_or_new_episode = TRUE
    ),

    cancer_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date
        FROM cancer_diagnoses_filtered
        GROUP BY person_id
    ),

    cancer_register_logic AS (
        SELECT
            diag.person_id,
            'Cancer' AS register_name,
            COALESCE(diag.earliest_diagnosis_date IS NOT NULL, FALSE) AS is_on_register
        FROM cancer_person_aggregates diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM cancer_register_logic

{% endmacro %}
