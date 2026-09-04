{% macro calculate_depression_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_depression_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Depression register status at a given reference date.

    Business Logic:
    - Age ≥18 at reference date
    - Latest first/new episode of depression on/after 2006-04-01
    - Unresolved (no DEPRES_COD after latest first/new episode)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH depression_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code,
            is_first_or_new_episode
        FROM {{ ref('int_depression_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    depression_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code AND is_first_or_new_episode THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code AND is_first_or_new_episode THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM depression_diagnoses_filtered
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

    depression_register_logic AS (
        SELECT
            diag.person_id,
            'Depression' AS register_name,
            COALESCE(
                age.age >= 18
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND diag.latest_diagnosis_date >= '2006-04-01'
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date > diag.latest_resolved_date
                ),
                FALSE
            ) AS is_on_register
        FROM depression_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM depression_register_logic

{% endmacro %}
