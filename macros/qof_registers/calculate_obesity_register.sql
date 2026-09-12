{% macro calculate_obesity_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_obesity_register.sql. This macro is strict as-of and derives age at the reference date; the live fact includes future-dated records. #}
    {#
    Calculates Obesity register status at a given reference date.

    Business Logic:
    - Age ≥18 at reference date
    - BMI ≥30 OR (BAME + BMI ≥27.5)
    - Uses ethnicity-adjusted BMI thresholds

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH bmi_data AS (
        -- int_bmi_qof is already person-level with latest BMI
        SELECT
            person_id,
            is_bmi_30_plus,
            is_bmi_27_5_plus,
            latest_valid_bmi_value,
            latest_valid_bmi_date
        FROM {{ ref('int_bmi_qof') }}
        WHERE latest_valid_bmi_date <= {{ reference_date_expr }}
    ),

    ethnicity_data AS (
        SELECT
            person_id,
            is_bame
        FROM {{ ref('int_ethnicity_qof') }}
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

    obesity_register_logic AS (
        SELECT
            bmi.person_id,
            'Obesity' AS register_name,
            COALESCE(
                age.age >= 18
                AND (
                    bmi.is_bmi_30_plus = TRUE
                    OR (eth.is_bame = TRUE AND bmi.is_bmi_27_5_plus = TRUE)
                ),
                FALSE
            ) AS is_on_register
        FROM bmi_data bmi
        LEFT JOIN age_at_reference age ON bmi.person_id = age.person_id
        LEFT JOIN ethnicity_data eth ON bmi.person_id = eth.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM obesity_register_logic

{% endmacro %}
