{% macro calculate_palliative_care_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_palliative_care_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Palliative Care register status at a given reference date.

    Business Logic:
    - Palliative care diagnosis on/after 1 April 2008
    - Not excluded by "no longer indicated" code (must be before/equal to diagnosis)
    - No age restrictions

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH palliative_care_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_palliative_care_code,
            is_palliative_care_not_indicated_code
        FROM {{ ref('int_palliative_care_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    palliative_care_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_palliative_care_code AND clinical_effective_date >= '2008-04-01' THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_palliative_care_not_indicated_code THEN clinical_effective_date END) AS latest_no_longer_indicated_date
        FROM palliative_care_diagnoses_filtered
        GROUP BY person_id
    ),

    palliative_care_register_logic AS (
        SELECT
            diag.person_id,
            'Palliative Care' AS register_name,
            COALESCE(
                diag.latest_diagnosis_date IS NOT NULL
                AND (diag.latest_no_longer_indicated_date IS NULL OR diag.latest_no_longer_indicated_date <= diag.latest_diagnosis_date),
                FALSE
            ) AS is_on_register
        FROM palliative_care_person_aggregates diag
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM palliative_care_register_logic

{% endmacro %}
