{# Pair: fct_person_learning_disability_register_under_14.sql. Clinical evidence is bounded by the reference date. #}
{% macro calculate_learning_disability_under_14_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {#
    Calculates Learning Disability (Under 14) register status at one or more reference dates.

    Business Logic:
    - Has learning disability diagnosis (LD_COD)
    - NOT excluded: no exclusion code (LDREM_COD) on or after latest diagnosis
    - Age below 14 at the reference date, capped at death

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    learning_disability_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_exclusion_code
        FROM {{ ref('int_learning_disability_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    learning_disability_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_exclusion_code THEN clinical_effective_date END) AS latest_exclusion_date
        FROM learning_disability_diagnoses_filtered
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
        FROM learning_disability_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    learning_disability_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Learning Disability (Under 14)' AS register_name,
            COALESCE(
                -- Must have an LD diagnosis
                age.age < 14
                AND diag.latest_diagnosis_date IS NOT NULL
                -- Must not have been excluded on or after latest diagnosis
                AND (
                    diag.latest_exclusion_date IS NULL
                    OR diag.latest_diagnosis_date::DATE > diag.latest_exclusion_date::DATE
                ),
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date
        FROM learning_disability_person_aggregates AS diag
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
        latest_diagnosis_date
    FROM learning_disability_register_logic

{% endmacro %}
