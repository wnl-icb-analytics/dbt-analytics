{% macro calculate_cvd_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_cvd_register.sql. Evidence is bounded by the reference date. #}
    {#
    Calculates QOF v51 CD_REG status at one or more reference dates by composing the CHD
    and stroke/TIA register macros.

    Component evidence dated or recorded after each reference date is excluded
    by the component macros.

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person on either component register at each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date (earliest qualifying component diagnosis),
        latest_diagnosis_date (latest qualifying component diagnosis)
    #}

    WITH chd_register AS (
        {{ calculate_chd_register(reference_date_expr=reference_date_expr, reference_dates=reference_dates) }}
    ),

    stroke_tia_register AS (
        {{ calculate_stroke_tia_register(reference_date_expr=reference_date_expr, reference_dates=reference_dates) }}
    ),

    cvd_members AS (
        SELECT reference_date, person_id, earliest_diagnosis_date, latest_diagnosis_date
        FROM chd_register
        WHERE is_on_register = TRUE

        UNION ALL

        SELECT reference_date, person_id, earliest_diagnosis_date, latest_diagnosis_date
        FROM stroke_tia_register
        WHERE is_on_register = TRUE
    )

    SELECT
        reference_date,
        person_id,
        'Cardiovascular Disease' AS register_name,
        TRUE AS is_on_register,
        MIN(earliest_diagnosis_date) AS earliest_diagnosis_date,
        MAX(latest_diagnosis_date) AS latest_diagnosis_date
    FROM cvd_members
    GROUP BY reference_date, person_id

{% endmacro %}
