{% macro calculate_ndh_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_ndh_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates NDH register status at one or more reference dates.

    Business Logic:
    - NDH_COD, IGT_COD or PRD_COD evidence known by the reference date
    - Age at least 18 years at the reference date, capped at death
    - No diabetes diagnosis, or latest diabetes resolution strictly later than the latest diabetes diagnosis
    - Gestational diabetes alone cannot add a person

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with ndh records known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    ndh_person_aggregates AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            MIN(diag.clinical_effective_date) AS earliest_diagnosis_date,
            MAX(diag.clinical_effective_date) AS latest_diagnosis_date
        FROM {{ ref('int_ndh_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, diag.person_id
    ),

    diabetes_status AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            MIN(CASE WHEN diag.is_general_diabetes_code THEN diag.clinical_effective_date END) AS earliest_diabetes_diagnosis_date,
            MAX(CASE WHEN diag.is_general_diabetes_code THEN diag.clinical_effective_date END) AS latest_diabetes_diagnosis_date,
            MAX(CASE WHEN diag.is_diabetes_resolved_code THEN diag.clinical_effective_date END) AS latest_diabetes_resolved_date
        FROM {{ ref('int_diabetes_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, diag.person_id
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
        FROM ndh_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    )

    SELECT
        diagnosis.reference_date,
        diagnosis.person_id,
        'NDH' AS register_name,
        COALESCE(
            age.age >= 18
            AND (
                diabetes.earliest_diabetes_diagnosis_date IS NULL
                OR diabetes.latest_diabetes_resolved_date > diabetes.latest_diabetes_diagnosis_date
            ),
            FALSE
        ) AS is_on_register,
        diagnosis.earliest_diagnosis_date,
        diagnosis.latest_diagnosis_date
    FROM ndh_person_aggregates AS diagnosis
    LEFT JOIN age_at_reference AS age
        ON diagnosis.person_id = age.person_id
        AND diagnosis.reference_date = age.reference_date
    LEFT JOIN diabetes_status AS diabetes
        ON diagnosis.person_id = diabetes.person_id
        AND diagnosis.reference_date = diabetes.reference_date

{% endmacro %}
