{% macro calculate_depression_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_depression_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Depression register status at one or more reference dates.

    Business Logic:
    - Age ≥18 at reference date
    - Latest first/new episode of depression on/after 2006-04-01
    - Unresolved (no DEPRES_COD after latest first/new episode)

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    depression_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code,
            is_first_or_new_episode
        FROM {{ ref('int_depression_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    depression_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code AND is_first_or_new_episode THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code AND is_first_or_new_episode THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM depression_diagnoses_filtered
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
        FROM depression_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    depression_register_logic AS (
        SELECT
            diag.reference_date,
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
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date
        FROM depression_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date
    FROM depression_register_logic

{% endmacro %}
