{% macro calculate_cyp_asthma_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_cyp_asthma_register.sql. This macro is strict as-of; the live fact uses evidence dated on or before today. #}
    {#
    Calculates CYP asthma register status at one or more reference dates.

    Business Logic:
    - Age <18 at reference date, capped at death
    - Active asthma diagnosis (latest diagnosis date after the latest resolution date, or no resolution)
    - Asthma medication after the 12-month boundary and on or before the reference date

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with an asthma record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_resolved_date,
        latest_medication_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    asthma_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code,
            diag.is_resolved_code
        FROM {{ ref('int_asthma_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    asthma_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
            COALESCE(
                MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
                AND (
                    MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NULL
                    OR MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)::DATE
                       > MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)::DATE
                ),
                FALSE
            ) AS has_active_asthma_diagnosis
        FROM asthma_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    asthma_medications_filtered AS (
        SELECT
            ref_date.reference_date,
            med.person_id,
            MAX(med.order_date) AS latest_medication_date
        FROM {{ ref('int_asthma_medications_all') }} AS med
        INNER JOIN reference_dates AS ref_date
            ON med.order_date > DATEADD('month', -12, ref_date.reference_date)
            AND {{ ltc_register_known_by('med.order_date', 'med.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, med.person_id
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
        FROM asthma_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    asthma_register_logic AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            'CYP Asthma' AS register_name,
            COALESCE(
                age.age < 18
                AND diag.has_active_asthma_diagnosis = TRUE
                AND med.latest_medication_date IS NOT NULL,
                FALSE
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date,
            diag.latest_resolved_date,
            med.latest_medication_date
        FROM asthma_person_aggregates AS diag
        LEFT JOIN age_at_reference AS age
            ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
        LEFT JOIN asthma_medications_filtered AS med
            ON diag.person_id = med.person_id
            AND diag.reference_date = med.reference_date
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
    FROM asthma_register_logic

{% endmacro %}
