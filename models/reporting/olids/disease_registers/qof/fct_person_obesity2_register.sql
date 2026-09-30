-- Pair: macros/qof_registers/calculate_obesity2_register.sql.
-- This live fact includes future-dated records. Its PIT pair is strict as-of
-- and derives age at the reference date rather than using current age.

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
QOF v51 OBES2 register. The live register intentionally includes future-dated
records; pit_obesity2_register applies the same rules at a supplied date.
*/

WITH bmi_status AS (
    SELECT
        person_id,
        MAX(CASE
            WHEN is_bmi_35_code OR bmi_value >= 35
                THEN clinical_effective_date
        END) AS latest_bmi_35_date,
        MAX(CASE
            WHEN source_cluster_id = 'BMIVAL_COD' AND bmi_value >= 32.5
                THEN clinical_effective_date
        END) AS latest_bmi_32_5_date
    FROM {{ ref('int_obesity2_bmi_all') }}
    WHERE clinical_effective_date > DATEADD(month, -12, CURRENT_DATE())
    GROUP BY person_id
),

ethnicity_status AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_ethnicity_date,
        MAX(CASE
            WHEN is_lower_bmi_threshold_ethnicity THEN clinical_effective_date
        END) AS latest_lower_threshold_ethnicity_date
    FROM {{ ref('int_obesity2_ethnicity_all') }}
    GROUP BY person_id
),

obesity2_diagnosis_status AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_ascvd_code THEN clinical_effective_date END)
            AS earliest_ascvd_date,
        MIN(CASE
            WHEN is_obstructive_sleep_apnoea_code THEN clinical_effective_date
        END) AS earliest_obstructive_sleep_apnoea_date
    FROM {{ ref('int_obesity2_diagnoses_all') }}
    GROUP BY person_id
),

hypertension_status AS (
    SELECT
        person_id,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_hypertension_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            AS latest_hypertension_resolved_date
    FROM {{ ref('int_hypertension_diagnoses_all') }}
    GROUP BY person_id
),

diabetes_status AS (
    SELECT
        person_id,
        MAX(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END)
            AS latest_type2_diabetes_date,
        MAX(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END)
            AS latest_diabetes_resolved_date
    FROM {{ ref('int_diabetes_diagnoses_all') }}
    GROUP BY person_id
),

latest_lipid_tests AS (
    SELECT *
    FROM {{ ref('int_obesity2_lipids_all') }}
    WHERE clinical_effective_date > DATEADD(month, -12, CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, source_cluster_id
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

lipid_test_status AS (
    SELECT
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
    GROUP BY person_id
),

lipid_therapy_status AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_lipid_therapy_date
    FROM {{ ref('int_obesity2_lipid_lowering_medications_all') }}
    WHERE order_date > DATEADD(month, -6, CURRENT_DATE())
    GROUP BY person_id
),

criteria AS (
    SELECT
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
            eth.latest_ethnicity_date = eth.latest_lower_threshold_ethnicity_date,
            FALSE
        ) AS has_lower_bmi_threshold_ethnicity,
        diag.earliest_ascvd_date IS NOT NULL AS has_ascvd,
        COALESCE(
            hyp.latest_hypertension_date IS NOT NULL
            AND (
                hyp.latest_hypertension_resolved_date IS NULL
                OR hyp.latest_hypertension_date
                    >= hyp.latest_hypertension_resolved_date
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
                OR dm.latest_type2_diabetes_date
                    >= dm.latest_diabetes_resolved_date
            ),
            FALSE
        ) AS has_unresolved_type2_diabetes
    FROM bmi_status AS bmi
    LEFT JOIN {{ ref('dim_person_age') }} AS age ON bmi.person_id = age.person_id
    LEFT JOIN {{ ref('dim_person_gender') }} AS gender
        ON bmi.person_id = gender.person_id
    LEFT JOIN ethnicity_status AS eth ON bmi.person_id = eth.person_id
    LEFT JOIN obesity2_diagnosis_status AS diag
        ON bmi.person_id = diag.person_id
    LEFT JOIN hypertension_status AS hyp ON bmi.person_id = hyp.person_id
    LEFT JOIN diabetes_status AS dm ON bmi.person_id = dm.person_id
    LEFT JOIN lipid_test_status AS lipid ON bmi.person_id = lipid.person_id
    LEFT JOIN lipid_therapy_status AS therapy ON bmi.person_id = therapy.person_id
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
),

register_logic AS (
    SELECT
        *,
        age >= 18 AS meets_age_criteria,
        COALESCE(
            age >= 18
            AND meets_bmi_criteria
            AND comorbidity_count >= 4,
            FALSE
        ) AS is_on_register
    FROM scored
)

SELECT
    person_id,
    age,
    gender,
    is_on_register,
    meets_age_criteria,
    meets_bmi_criteria,
    latest_bmi_35_date,
    latest_bmi_32_5_date,
    has_lower_bmi_threshold_ethnicity,
    latest_ethnicity_date,
    latest_lower_threshold_ethnicity_date,
    comorbidity_count,
    has_ascvd,
    earliest_ascvd_date,
    has_unresolved_hypertension,
    latest_hypertension_date,
    latest_hypertension_resolved_date,
    has_dyslipidaemia,
    latest_lipid_therapy_date,
    latest_ldl_date,
    latest_ldl_value,
    latest_triglycerides_date,
    latest_triglycerides_value,
    latest_hdl_date,
    latest_hdl_value,
    has_obstructive_sleep_apnoea,
    earliest_obstructive_sleep_apnoea_date,
    has_unresolved_type2_diabetes,
    latest_type2_diabetes_date,
    latest_diabetes_resolved_date
FROM register_logic
WHERE is_on_register
