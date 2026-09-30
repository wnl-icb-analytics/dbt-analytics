{% macro calculate_obesity_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_obesity_register.sql. This macro is strict as-of and derives age at the reference date; the live fact includes future-dated records. #}
    {#
    Calculates Obesity register status at one or more reference dates.

    Business Logic:
    - Age ≥18 at reference date
    - BMI ≥30 OR (BAME + BMI ≥27.5)
    - Uses ethnicity-adjusted BMI thresholds

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with numeric or coded BMI evidence and a valid
        BMI record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    bmi_events AS (
        -- Latest-record rules of int_bmi_qof, applied to the records known by each reference date.
        SELECT
            ref_date.reference_date,
            bmi.id,
            bmi.person_id,
            bmi.clinical_effective_date,
            bmi.is_valid_bmi,
            bmi.is_bmi_30_plus,
            bmi.is_bmi_27_5_plus
        FROM {{ ref('int_bmi_qof_all') }} AS bmi
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('bmi.clinical_effective_date', 'bmi.date_recorded', 'ref_date.reference_date') }}
    ),

    bmi_dates AS (
        SELECT
            reference_date,
            person_id,
            MAX(clinical_effective_date) AS latest_diagnosis_date,
            MAX(CASE WHEN is_valid_bmi THEN clinical_effective_date END) AS latest_valid_bmi_date
        FROM bmi_events
        GROUP BY reference_date, person_id
    ),

    bmi_data AS (
        -- Flags come from the latest BMI record, valid or not, as in int_bmi_qof.
        SELECT
            reference_date,
            person_id,
            is_bmi_30_plus,
            is_bmi_27_5_plus
        FROM bmi_events
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY reference_date, person_id ORDER BY clinical_effective_date DESC, id DESC
        ) = 1
    ),

    ethnicity_data AS (
        -- Latest QOF ethnicity record known by each reference date, as in int_ethnicity_qof.
        SELECT
            ref_date.reference_date,
            eth.person_id,
            eth.is_bame
        FROM {{ ref('int_ethnicity_qof_all') }} AS eth
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('eth.clinical_effective_date', 'eth.date_recorded', 'ref_date.reference_date') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ref_date.reference_date, eth.person_id ORDER BY eth.clinical_effective_date DESC, eth.id DESC
        ) = 1
    ),

    age_at_reference AS (
        SELECT
            bmi.reference_date,
            bmi.person_id,
            FLOOR(DATEDIFF(
                'month',
                birth.birth_date_approx,
                CASE
                    WHEN birth.death_date_approx <= bmi.reference_date THEN birth.death_date_approx
                    ELSE bmi.reference_date
                END
            ) / 12) AS age
        FROM bmi_data AS bmi
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON bmi.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    obesity_register_logic AS (
        SELECT
            bmi.reference_date,
            bmi.person_id,
            'Obesity' AS register_name,
            COALESCE(
                age.age >= 18
                AND (
                    bmi.is_bmi_30_plus = TRUE
                    OR (eth.is_bame = TRUE AND bmi.is_bmi_27_5_plus = TRUE)
                ),
                FALSE
            ) AS is_on_register,
            -- fct_person_ltc_summary uses latest valid BMI as the earliest diagnosis date.
            dates.latest_valid_bmi_date AS earliest_diagnosis_date,
            dates.latest_diagnosis_date
        FROM bmi_data AS bmi
        INNER JOIN bmi_dates AS dates
            ON bmi.person_id = dates.person_id
            AND bmi.reference_date = dates.reference_date
            -- The old macro omitted people with no valid BMI date.
            AND dates.latest_valid_bmi_date IS NOT NULL
        LEFT JOIN age_at_reference age ON bmi.person_id = age.person_id
            AND bmi.reference_date = age.reference_date
        LEFT JOIN ethnicity_data eth ON bmi.person_id = eth.person_id
            AND bmi.reference_date = eth.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM obesity_register_logic

{% endmacro %}
