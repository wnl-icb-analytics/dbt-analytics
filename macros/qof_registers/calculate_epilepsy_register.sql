{% macro calculate_epilepsy_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_epilepsy_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates Epilepsy register status at one or more reference dates.

    Business Logic:
    - Age ≥18 at reference date
    - Active epilepsy diagnosis (latest diagnosis > latest resolution)
    - Recent medication within 6 months from reference date

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date,
        latest_medication_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    epilepsy_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_epilepsy_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    epilepsy_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM epilepsy_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    age_at_reference AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            FLOOR(DATEDIFF('month', birth.birth_date_approx, diag.reference_date) / 12) AS age
        FROM epilepsy_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    epilepsy_medications_filtered AS (
        SELECT
            ref_date.reference_date,
            meds.person_id,
            order_date
        FROM {{ ref('int_epilepsy_medications_all') }} AS meds
        INNER JOIN reference_dates AS ref_date
            ON meds.order_date <= ref_date.reference_date
            AND meds.order_date >= DATEADD('month', -6, ref_date.reference_date)
    ),

    epilepsy_medications_aggregates AS (
        SELECT
            reference_date,
            person_id,
            COUNT(*) AS recent_medication_count,
            MAX(order_date) AS latest_medication_date
        FROM epilepsy_medications_filtered
        GROUP BY reference_date, person_id
    ),

    epilepsy_register_logic AS (
        SELECT
            diag.reference_date,
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
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date,
            meds.latest_medication_date
        FROM epilepsy_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
        LEFT JOIN epilepsy_medications_aggregates meds ON diag.person_id = meds.person_id
            AND diag.reference_date = meds.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date,
        latest_medication_date
    FROM epilepsy_register_logic

{% endmacro %}
