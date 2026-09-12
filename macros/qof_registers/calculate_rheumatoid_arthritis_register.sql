{% macro calculate_rheumatoid_arthritis_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_rheumatoid_arthritis_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Rheumatoid Arthritis register status at a given reference date.

    QOF v50 RA_REG (CQRS 001):
    - PAT_AGE >= 16 at the achievement/reference date (NOT age at diagnosis)
    - RARTH_DAT ≠ Null (any RARTH_COD diagnosis; no resolution codes in RA)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH ra_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_rheumatoid_arthritis_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    ra_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date
        FROM ra_diagnoses_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            -- Completed years at the reference date (month-accurate, matching dim_person_age)
            -- so a patient is not counted a year older before their birthday.
            FLOOR(DATEDIFF('month', birth_date_approx, {{ reference_date_expr }}) / 12) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    ra_register_logic AS (
        SELECT
            diag.person_id,
            'Rheumatoid Arthritis' AS register_name,
            -- PAT_AGE >= 16 at reference date AND a RARTH_COD diagnosis on record
            COALESCE(
                diag.earliest_diagnosis_date IS NOT NULL
                AND age.age >= 16,
                FALSE
            ) AS is_on_register
        FROM ra_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM ra_register_logic

{% endmacro %}
