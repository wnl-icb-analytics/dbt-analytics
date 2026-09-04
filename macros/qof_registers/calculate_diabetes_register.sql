{% macro calculate_diabetes_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_diabetes_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates Diabetes register status at a given reference date.

    Business Logic:
    - Age ≥17 at reference date
    - Active diabetes diagnosis (latest diagnosis > latest resolution)
    - Type classification (Type 1 vs Type 2 vs Unknown)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, practice_code, register_name, is_on_register, diabetes_type
    #}

    WITH diabetes_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_general_diabetes_code,
            is_type1_diabetes_code,
            is_type2_diabetes_code,
            is_diabetes_resolved_code
        FROM {{ ref('int_diabetes_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    diabetes_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END) AS latest_type1_date,
            MAX(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END) AS latest_type2_date,
            MAX(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM diabetes_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            FLOOR(DATEDIFF(
                'month',
                birth_date_approx,
                CASE
                    WHEN death_date_approx <= {{ reference_date_expr }} THEN death_date_approx
                    ELSE {{ reference_date_expr }}
                END
            ) / 12) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    diabetes_register_logic AS (
        SELECT
            diag.person_id,
            'Diabetes' AS register_name,
            COALESCE(
                age.age >= 17
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_resolved_date
                ),
                FALSE
            ) AS is_on_register,
            CASE
                WHEN COALESCE(
                    age.age >= 17
                    AND diag.earliest_diagnosis_date IS NOT NULL
                    AND (
                        diag.latest_resolved_date IS NULL
                        OR diag.latest_diagnosis_date > diag.latest_resolved_date
                    ),
                    FALSE
                ) = FALSE THEN NULL
                WHEN diag.latest_type1_date IS NOT NULL
                    AND (diag.latest_type2_date IS NULL OR diag.latest_type1_date >= diag.latest_type2_date)
                    THEN 'Type 1'
                WHEN diag.latest_type2_date IS NOT NULL
                    AND (diag.latest_type1_date IS NULL OR diag.latest_type2_date > diag.latest_type1_date)
                    THEN 'Type 2'
                ELSE 'Unknown'
            END AS diabetes_type
        FROM diabetes_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register,
        diabetes_type
    FROM diabetes_register_logic

{% endmacro %}
