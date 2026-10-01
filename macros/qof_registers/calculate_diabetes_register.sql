{% macro calculate_diabetes_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_diabetes_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates Diabetes register status at one or more reference dates.

    Business Logic:
    - Age ≥17 at reference date
    - Active diabetes diagnosis (no resolution after the latest diagnosis; same-day resolution retains it)
    - Type classification (Type 1 vs Type 2 vs Unknown)

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a diabetes record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date,
        diabetes_type, earliest_type1_date, latest_type1_date,
        earliest_type2_date, latest_type2_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    diabetes_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_general_diabetes_code,
            diag.is_type1_diabetes_code,
            diag.is_type2_diabetes_code,
            diag.is_diabetes_resolved_code
        FROM {{ ref('int_diabetes_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    diabetes_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MIN(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END) AS earliest_type1_date,
            MAX(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END) AS latest_type1_date,
            MIN(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END) AS earliest_type2_date,
            MAX(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END) AS latest_type2_date,
            MAX(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM diabetes_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    age_at_reference AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            FLOOR(DATEDIFF(
                'month',
                birth.birth_date_approx,
                CASE
                    WHEN birth.death_date_approx <= diag.reference_date THEN birth.death_date_approx
                    ELSE diag.reference_date
                END
            ) / 12) AS age
        FROM diabetes_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    diabetes_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Diabetes' AS register_name,
            COALESCE(
                age.age >= 17
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND (
                    diag.latest_resolved_date IS NULL
                    OR diag.latest_diagnosis_date::DATE >= diag.latest_resolved_date::DATE
                ),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date,
            CASE
                WHEN COALESCE(
                    age.age >= 17
                    AND diag.earliest_diagnosis_date IS NOT NULL
                    AND (
                        diag.latest_resolved_date IS NULL
                        OR diag.latest_diagnosis_date::DATE >= diag.latest_resolved_date::DATE
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
            END AS diabetes_type,
            diag.earliest_type1_date,
            diag.latest_type1_date,
            diag.earliest_type2_date,
            diag.latest_type2_date
        FROM diabetes_person_aggregates AS diag
        LEFT JOIN age_at_reference AS age
            ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date,
        diabetes_type,
        earliest_type1_date,
        latest_type1_date,
        earliest_type2_date,
        latest_type2_date
    FROM diabetes_register_logic

{% endmacro %}
