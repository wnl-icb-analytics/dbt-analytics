{% macro calculate_obesity_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_obesity_register.sql. This macro is strict as-of and derives age at the reference date; the live fact includes future-dated records. #}
    {#
    Calculates Obesity register status at one or more reference dates.

    Business Logic:
    - Age ≥18 at reference date
    - Latest valid BMI (5 to 400) known by the reference date, at any date: ≥30, or ≥27.5
      for ethnic groups at higher cardiometabolic risk
    - No QOF 12-month window: an unmeasured person keeps their last valid BMI

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

    bmi_records AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_valid_bmi,
            is_bmi_30_plus,
            is_bmi_27_5_plus,
            {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date,
            -- Latest record: clinical time, then observation id and cluster, as in int_bmi_qof.
            TO_VARCHAR(clinical_effective_date, 'YYYY-MM-DD HH24:MI:SS.FF9')
                || '|' || TO_VARCHAR(id) || '|' || source_cluster_id AS record_key
        FROM {{ ref('int_bmi_qof_all') }}
        WHERE clinical_effective_date IS NOT NULL
    ),

    latest_valid_bmi AS (
        {{ ltc_latest_known_record("SELECT person_id, record_key, known_date FROM bmi_records WHERE is_valid_bmi") }}
    ),

    latest_any_bmi AS (
        {{ ltc_latest_known_record("SELECT person_id, record_key, known_date FROM bmi_records") }}
    ),

    bmi_data AS (
        -- Flags and date of the latest valid BMI, and the date of the latest BMI record
        -- of any kind, known by each reference date.
        SELECT
            valid.reference_date,
            valid.person_id,
            valid_record.is_bmi_30_plus,
            valid_record.is_bmi_27_5_plus,
            valid_record.clinical_effective_date AS latest_valid_bmi_date,
            any_record.clinical_effective_date AS latest_bmi_date
        FROM latest_valid_bmi AS valid
        INNER JOIN bmi_records AS valid_record
            ON valid.person_id = valid_record.person_id
            AND valid.record_key = valid_record.record_key
        INNER JOIN latest_any_bmi AS latest_any
            ON valid.person_id = latest_any.person_id
            AND valid.reference_date = latest_any.reference_date
        INNER JOIN bmi_records AS any_record
            ON latest_any.person_id = any_record.person_id
            AND latest_any.record_key = any_record.record_key
    ),

    ethnicity_records AS (
        SELECT
            person_id,
            is_bame,
            {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date,
            -- Latest record: clinical date, then observation id and cluster, as in int_ethnicity_qof.
            TO_VARCHAR(CAST(clinical_effective_date AS TIMESTAMP_NTZ), 'YYYY-MM-DD HH24:MI:SS.FF9')
                || '|' || TO_VARCHAR(id) || '|' || cluster_id AS record_key
        FROM {{ ref('int_ethnicity_qof_all') }}
        WHERE clinical_effective_date IS NOT NULL
    ),

    latest_ethnicity AS (
        {{ ltc_latest_known_record("SELECT person_id, record_key, known_date FROM ethnicity_records") }}
    ),

    ethnicity_data AS (
        -- Latest QOF ethnicity record known by each reference date.
        SELECT
            latest.reference_date,
            latest.person_id,
            record.is_bame
        FROM latest_ethnicity AS latest
        INNER JOIN ethnicity_records AS record
            ON latest.person_id = record.person_id
            AND latest.record_key = record.record_key
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
            bmi.latest_valid_bmi_date AS earliest_diagnosis_date,
            bmi.latest_bmi_date AS latest_diagnosis_date
        FROM bmi_data AS bmi
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
