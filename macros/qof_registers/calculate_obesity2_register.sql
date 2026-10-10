{% macro calculate_obesity2_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_obesity2_register.sql. Evidence is bounded by the reference date. #}
    {# QOF v51 OBES2 register evaluated strictly at the supplied date. #}

    {#
    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with BMI evidence in the preceding 12 months
        known by each reference date:
        reference_date, person_id, register_name, is_on_register
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    bmi_events AS (
        SELECT ref_date.reference_date, event.*
        FROM {{ ref('int_obesity2_bmi_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON CAST(event.clinical_effective_date AS DATE) > DATEADD(month, -12, ref_date.reference_date)
          AND {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    bmi_status AS (
        SELECT
            reference_date,
            person_id,
            MAX(CASE
                WHEN is_bmi_35_code OR bmi_value >= 35
                    THEN clinical_effective_date
            END) AS latest_bmi_35_date,
            MAX(CASE
                WHEN source_cluster_id = 'BMIVAL_COD' AND bmi_value >= 32.5
                    THEN clinical_effective_date
            END) AS latest_bmi_32_5_date
        FROM bmi_events
        GROUP BY reference_date, person_id
    ),

    ethnicity_events AS (
        SELECT ref_date.reference_date, event.*
        FROM {{ ref('int_obesity2_ethnicity_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    ethnicity_status AS (
        SELECT
            reference_date,
            person_id,
            MAX(clinical_effective_date) AS latest_ethnicity_date,
            MAX(CASE
                WHEN is_lower_bmi_threshold_ethnicity
                    THEN clinical_effective_date
            END) AS latest_lower_threshold_ethnicity_date
        FROM ethnicity_events
        GROUP BY reference_date, person_id
    ),

    obesity2_diagnoses AS (
        SELECT ref_date.reference_date, event.*
        FROM {{ ref('int_obesity2_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    obesity2_diagnosis_status AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_ascvd_code THEN clinical_effective_date END)
                AS earliest_ascvd_date,
            MIN(CASE
                WHEN is_obstructive_sleep_apnoea_code
                    THEN clinical_effective_date
            END) AS earliest_obstructive_sleep_apnoea_date
        FROM obesity2_diagnoses
        GROUP BY reference_date, person_id
    ),

    hypertension_status AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
                AS latest_hypertension_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
                AS latest_hypertension_resolved_date
        FROM {{ ref('int_hypertension_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, event.person_id
    ),

    diabetes_status AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            MAX(CASE
                WHEN is_type2_diabetes_code THEN clinical_effective_date
            END) AS latest_type2_diabetes_date,
            MAX(CASE
                WHEN is_diabetes_resolved_code THEN clinical_effective_date
            END) AS latest_diabetes_resolved_date
        FROM {{ ref('int_diabetes_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
        GROUP BY ref_date.reference_date, event.person_id
    ),

    latest_lipid_tests AS (
        SELECT ref_date.reference_date, event.*
        FROM {{ ref('int_obesity2_lipids_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON CAST(event.clinical_effective_date AS DATE) > DATEADD(month, -12, ref_date.reference_date)
          AND {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ref_date.reference_date, event.person_id, source_cluster_id
            ORDER BY CAST(clinical_effective_date AS DATE) DESC, id DESC
        ) = 1
    ),

    lipid_test_status AS (
        SELECT
            reference_date,
            person_id,
            MAX(CASE WHEN source_cluster_id = 'LDLCCHOL_COD'
                THEN clinical_effective_date END) AS latest_ldl_date,
            MAX(CASE WHEN source_cluster_id = 'LDLCCHOL_COD'
                THEN lipid_value END) AS latest_ldl_value,
            MAX(CASE WHEN source_cluster_id = 'TRIGLYC_COD'
                THEN clinical_effective_date END) AS latest_triglycerides_date,
            MAX(CASE WHEN source_cluster_id = 'TRIGLYC_COD'
                THEN lipid_value END) AS latest_triglycerides_value,
            MAX(CASE WHEN source_cluster_id = 'HDLCCHOL_COD'
                THEN clinical_effective_date END) AS latest_hdl_date,
            MAX(CASE WHEN source_cluster_id = 'HDLCCHOL_COD'
                THEN lipid_value END) AS latest_hdl_value
        FROM latest_lipid_tests
        GROUP BY reference_date, person_id
    ),

    lipid_therapy_status AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            MAX(order_date) AS latest_lipid_therapy_date
        FROM {{ ref('int_obesity2_lipid_lowering_medications_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON event.order_date > DATEADD(month, -6, ref_date.reference_date)
          AND event.order_date <= ref_date.reference_date
          AND (event.date_recorded IS NULL OR CAST(event.date_recorded AS DATE) <= ref_date.reference_date)
        GROUP BY ref_date.reference_date, event.person_id
    ),

    age_at_reference AS (
        SELECT
            bmi.reference_date,
            bmi.person_id,
            FLOOR(
                DATEDIFF('month', birth.birth_date_approx, bmi.reference_date) / 12
            ) AS age
        FROM bmi_status AS bmi
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON bmi.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    criteria AS (
        SELECT
            bmi.reference_date,
            bmi.person_id,
            age.age,
            gender.gender,
            bmi.latest_bmi_35_date,
            bmi.latest_bmi_32_5_date,
            eth.latest_ethnicity_date,
            eth.latest_lower_threshold_ethnicity_date,
            diag.earliest_ascvd_date,
            hyp.latest_hypertension_date,
            hyp.latest_hypertension_resolved_date,
            diag.earliest_obstructive_sleep_apnoea_date,
            dm.latest_type2_diabetes_date,
            dm.latest_diabetes_resolved_date,
            therapy.latest_lipid_therapy_date,
            lipid.latest_ldl_date,
            lipid.latest_ldl_value,
            lipid.latest_triglycerides_date,
            lipid.latest_triglycerides_value,
            lipid.latest_hdl_date,
            lipid.latest_hdl_value,
            COALESCE(
                CAST(eth.latest_ethnicity_date AS DATE)
                    = CAST(eth.latest_lower_threshold_ethnicity_date AS DATE),
                FALSE
            ) AS has_lower_bmi_threshold_ethnicity,
            diag.earliest_ascvd_date IS NOT NULL AS has_ascvd,
            COALESCE(
                hyp.latest_hypertension_date IS NOT NULL
                AND (
                    hyp.latest_hypertension_resolved_date IS NULL
                    OR CAST(hyp.latest_hypertension_date AS DATE)
                        >= CAST(hyp.latest_hypertension_resolved_date AS DATE)
                ),
                FALSE
            ) AS has_unresolved_hypertension,
            COALESCE(
                therapy.latest_lipid_therapy_date IS NOT NULL
                OR lipid.latest_ldl_value >= 4.1
                OR lipid.latest_triglycerides_value >= 1.7
                OR (
                    gender.gender IN ('Male', 'M')
                    AND lipid.latest_hdl_value < 1.0
                )
                OR (
                    gender.gender IN ('Female', 'F')
                    AND lipid.latest_hdl_value < 1.3
                ),
                FALSE
            ) AS has_dyslipidaemia,
            diag.earliest_obstructive_sleep_apnoea_date IS NOT NULL
                AS has_obstructive_sleep_apnoea,
            COALESCE(
                dm.latest_type2_diabetes_date IS NOT NULL
                AND (
                    dm.latest_diabetes_resolved_date IS NULL
                    OR CAST(dm.latest_type2_diabetes_date AS DATE)
                        >= CAST(dm.latest_diabetes_resolved_date AS DATE)
                ),
                FALSE
            ) AS has_unresolved_type2_diabetes
        FROM bmi_status AS bmi
        LEFT JOIN age_at_reference AS age ON bmi.person_id = age.person_id
            AND bmi.reference_date = age.reference_date
        LEFT JOIN {{ ref('dim_person_gender') }} AS gender
            ON bmi.person_id = gender.person_id
        LEFT JOIN ethnicity_status AS eth ON bmi.person_id = eth.person_id
            AND bmi.reference_date = eth.reference_date
        LEFT JOIN obesity2_diagnosis_status AS diag
            ON bmi.person_id = diag.person_id
            AND bmi.reference_date = diag.reference_date
        LEFT JOIN hypertension_status AS hyp ON bmi.person_id = hyp.person_id
            AND bmi.reference_date = hyp.reference_date
        LEFT JOIN diabetes_status AS dm ON bmi.person_id = dm.person_id
            AND bmi.reference_date = dm.reference_date
        LEFT JOIN lipid_test_status AS lipid ON bmi.person_id = lipid.person_id
            AND bmi.reference_date = lipid.reference_date
        LEFT JOIN lipid_therapy_status AS therapy
            ON bmi.person_id = therapy.person_id
            AND bmi.reference_date = therapy.reference_date
    ),

    scored AS (
        SELECT
            *,
            has_ascvd::INT
                + has_unresolved_hypertension::INT
                + has_dyslipidaemia::INT
                + has_obstructive_sleep_apnoea::INT
                + has_unresolved_type2_diabetes::INT AS comorbidity_count,
            COALESCE(
                latest_bmi_35_date IS NOT NULL
                OR (
                    has_lower_bmi_threshold_ethnicity
                    AND latest_bmi_32_5_date IS NOT NULL
                ),
                FALSE
            ) AS meets_bmi_criteria
        FROM criteria
    )

    SELECT
        reference_date,
        person_id,
        'Obesity2' AS register_name,
        COALESCE(
            age >= 18
            AND meets_bmi_criteria
            AND comorbidity_count >= 4,
            FALSE
        ) AS is_on_register
    FROM scored
{% endmacro %}
