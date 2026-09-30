{% macro calculate_familial_hypercholesterolaemia_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_familial_hypercholesterolaemia_register.sql. This macro is strict as-of; the live fact includes future-dated records and filters to current active patients. #}
    {#
    Calculates Familial hypercholesterolaemia register status at one or more reference dates.

    Business Logic:
    - FHYP_COD diagnosis known by the reference date
    - Estimated age at first diagnosis is at least 20 years, using age at the reference date capped at death
    - No resolution codes
    Current active-patient filtering is omitted; monthly models apply registration
    and living status at each month-end.

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with familial hypercholesterolaemia records known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    familial_hypercholesterolaemia_person_aggregates AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            MIN(CASE WHEN diag.is_diagnosis_code THEN diag.clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN diag.is_diagnosis_code THEN diag.clinical_effective_date END) AS latest_diagnosis_date
        FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, diag.person_id
    ),

    age_at_diagnosis AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            FLOOR(DATEDIFF('month', birth.birth_date_approx, diag.earliest_diagnosis_date) / 12) AS age_at_first_diagnosis
        FROM familial_hypercholesterolaemia_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    register_inclusion AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            -- Age at first diagnosis, from the birth date, so it does not change with the reference date.
            age.age_at_first_diagnosis AS age_at_first_fh_diagnosis
        FROM familial_hypercholesterolaemia_person_aggregates AS diag
        LEFT JOIN age_at_diagnosis AS age
            ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
    )

    SELECT
        reference_date,
        person_id,
        'Familial hypercholesterolaemia' AS register_name,
        -- Register logic: Include if has diagnosis and estimated age >=20 at first diagnosis
        COALESCE(
            earliest_diagnosis_date IS NOT NULL
            AND age_at_first_fh_diagnosis >= 20,
            FALSE
        ) AS is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM register_inclusion

{% endmacro %}
