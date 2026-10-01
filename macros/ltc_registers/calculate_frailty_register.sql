{% macro calculate_frailty_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_frailty_register.sql. This macro is strict as-of; the live fact includes future-dated records. #}
    {#
    Calculates Frailty register status at one or more reference dates.

    Business Logic:
    - Any MILDFRAIL_COD, MODFRAIL_COD or SEVFRAIL_COD diagnosis known by the reference date
    - Latest coded severity wins; same-date ties prefer greater severity, then the higher observation ID
    - No age restriction or resolution codes; calculated frailty scores are not used

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with frailty records known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, latest_frailty_severity, mild_frailty_count, moderate_frailty_count, severe_frailty_count
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    frailty_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.id,
            diag.clinical_effective_date,
            diag.is_diagnosis_code,
            diag.frailty_severity
        FROM {{ ref('int_frailty_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    latest_severity AS (
        -- Get the latest frailty severity for each person
        SELECT
            reference_date,
            person_id,
            frailty_severity AS latest_frailty_severity
        FROM frailty_diagnoses_filtered
        WHERE is_diagnosis_code = TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY reference_date, person_id
            ORDER BY
                clinical_effective_date DESC,
                CASE frailty_severity WHEN 'Severe' THEN 3 WHEN 'Moderate' THEN 2 ELSE 1 END DESC,
                id DESC
        ) = 1
    ),

    frailty_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
            -- Severity-specific counts
            COUNT(CASE WHEN frailty_severity = 'Mild' THEN 1 END) AS mild_frailty_count,
            COUNT(CASE WHEN frailty_severity = 'Moderate' THEN 1 END) AS moderate_frailty_count,
            COUNT(CASE WHEN frailty_severity = 'Severe' THEN 1 END) AS severe_frailty_count
        FROM frailty_diagnoses_filtered
        GROUP BY reference_date, person_id
    )

    SELECT
        diag.reference_date,
        diag.person_id,
        'Frailty' AS register_name,
        diag.earliest_diagnosis_date IS NOT NULL AS is_on_register,
        diag.earliest_diagnosis_date,
        diag.latest_diagnosis_date,
        severity.latest_frailty_severity,
        diag.mild_frailty_count,
        diag.moderate_frailty_count,
        diag.severe_frailty_count
    FROM frailty_person_aggregates AS diag
    LEFT JOIN latest_severity AS severity
        ON diag.person_id = severity.person_id
        AND diag.reference_date = severity.reference_date

{% endmacro %}
