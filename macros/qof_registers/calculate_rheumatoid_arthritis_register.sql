{% macro calculate_rheumatoid_arthritis_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_rheumatoid_arthritis_register.sql. Evidence is bounded by the reference date. #}
    {#
    Calculates Rheumatoid Arthritis register status at one or more reference dates.

    QOF v51 RA_REG (CQRS 001):
    - PAT_AGE >= 16 at the achievement/reference date (NOT age at diagnosis)
    - RARTH_DAT ≠ Null (any RARTH_COD diagnosis; no resolution codes in RA)

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

    ra_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_rheumatoid_arthritis_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    ra_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date
        FROM ra_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    age_at_reference AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            -- Month-based age calculation, matching dim_person_age.
            FLOOR(DATEDIFF('month', birth.birth_date_approx, diag.reference_date) / 12) AS age
        FROM ra_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    ra_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'Rheumatoid Arthritis' AS register_name,
            -- PAT_AGE >= 16 at reference date AND a RARTH_COD diagnosis on record
            COALESCE(
                diag.earliest_diagnosis_date IS NOT NULL
                AND age.age >= 16,
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date
        FROM ra_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM ra_register_logic

{% endmacro %}
