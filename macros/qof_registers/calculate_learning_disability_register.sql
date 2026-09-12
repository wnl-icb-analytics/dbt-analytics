{% macro calculate_learning_disability_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_learning_disability_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates Learning Disability register status at a given reference date.

    Business Logic (QOF v50):
    - Has learning disability diagnosis (LD_COD)
    - NOT excluded: no exclusion code (LDREM_COD) after latest diagnosis
    - No age restriction in QOF spec (includes all ages)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH learning_disability_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_exclusion_code
        FROM {{ ref('int_learning_disability_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    learning_disability_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_exclusion_code THEN clinical_effective_date END) AS latest_exclusion_date
        FROM learning_disability_diagnoses_filtered
        GROUP BY person_id
    ),

    learning_disability_register_logic AS (
        SELECT
            diag.person_id,
            'Learning Disability' AS register_name,
            COALESCE(
                -- Must have an LD diagnosis
                diag.latest_diagnosis_date IS NOT NULL
                -- Must not have been excluded after latest diagnosis
                AND (
                    diag.latest_exclusion_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_exclusion_date
                ),
                FALSE
            ) AS is_on_register
        FROM learning_disability_person_aggregates diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM learning_disability_register_logic

{% endmacro %}
