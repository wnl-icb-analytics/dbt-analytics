{% macro calculate_epilepsy_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_epilepsy_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates Epilepsy register status at a given reference date.

    Business Logic:
    - Age ≥18 at reference date
    - Active epilepsy diagnosis (latest diagnosis > latest resolution)
    - Recent medication within 6 months from reference date

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH epilepsy_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_epilepsy_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    epilepsy_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM epilepsy_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            FLOOR(DATEDIFF('month', birth_date_approx, {{ reference_date_expr }}) / 12) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    epilepsy_medications_filtered AS (
        SELECT
            person_id,
            order_date
        FROM {{ ref('int_epilepsy_medications_all') }}
        WHERE order_date <= {{ reference_date_expr }}
          AND order_date >= DATEADD('month', -6, {{ reference_date_expr }})
    ),

    epilepsy_medications_aggregates AS (
        SELECT
            person_id,
            COUNT(*) AS recent_medication_count
        FROM epilepsy_medications_filtered
        GROUP BY person_id
    ),

    epilepsy_register_logic AS (
        SELECT
            diag.person_id,
            'Epilepsy' AS register_name,
            COALESCE(
                age.age >= 18
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_resolved_date
                )
                AND meds.recent_medication_count > 0,
                FALSE
            ) AS is_on_register
        FROM epilepsy_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
        LEFT JOIN epilepsy_medications_aggregates meds ON diag.person_id = meds.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM epilepsy_register_logic

{% endmacro %}
