{{
    config(
        materialized='table',
        tags=['intermediate', 'programme', 'tirzepatide'],
        cluster_by=['person_id'])
}}

/*
One row per living, currently registered adult candidate in QOF v51 OBES2.
The most recent qualifying BMI evidence within 12 months supplies the cohort.
*/

WITH obesity2 AS (
    SELECT
        person_id,
        age,
        has_lower_bmi_threshold_ethnicity,
        has_unresolved_hypertension,
        has_dyslipidaemia,
        has_obstructive_sleep_apnoea,
        has_ascvd,
        has_unresolved_type2_diabetes,
        comorbidity_count
    FROM {{ ref('fct_person_obesity2_register') }}
),

qualifying_bmi_evidence AS (
    SELECT
        bmi.person_id,
        bmi.clinical_effective_date AS latest_bmi_date,
        bmi.bmi_value AS latest_bmi_value,
        bmi.source_cluster_id AS latest_bmi_source_cluster_id,
        bmi.is_bmi_35_code AS latest_bmi_is_bmi_35_code
    FROM {{ ref('int_obesity2_bmi_all') }} AS bmi
    INNER JOIN obesity2 AS obes ON bmi.person_id = obes.person_id
    WHERE bmi.clinical_effective_date > DATEADD(month, -12, CURRENT_DATE())
      AND bmi.clinical_effective_date <= CURRENT_DATE()
      AND (
          bmi.date_recorded IS NULL
          OR CAST(bmi.date_recorded AS DATE) <= CURRENT_DATE()
      )
      AND (
          bmi.is_bmi_35_code
          OR bmi.bmi_value >= 35
          OR (
              obes.has_lower_bmi_threshold_ethnicity
              AND bmi.bmi_value >= 32.5
          )
      )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY bmi.person_id
        ORDER BY bmi.clinical_effective_date DESC, bmi.id DESC
    ) = 1
),

cohorted AS (
    SELECT
        obes.person_id,
        obes.age,
        bmi.latest_bmi_value,
        bmi.latest_bmi_date,
        bmi.latest_bmi_source_cluster_id,
        bmi.latest_bmi_is_bmi_35_code,
        obes.has_lower_bmi_threshold_ethnicity,
        obes.has_unresolved_hypertension,
        obes.has_dyslipidaemia,
        obes.has_obstructive_sleep_apnoea,
        obes.has_ascvd,
        obes.has_unresolved_type2_diabetes,
        obes.comorbidity_count AS qualifying_comorbidity_count,
        COALESCE(
            bmi.latest_bmi_value >= CASE
                WHEN obes.has_lower_bmi_threshold_ethnicity THEN 37.5
                ELSE 40
            END,
            FALSE
        ) AS bmi_meets_cohort_1,
        COALESCE(
            bmi.latest_bmi_value >= CASE
                WHEN obes.has_lower_bmi_threshold_ethnicity THEN 32.5
                ELSE 35
            END
            AND bmi.latest_bmi_value < CASE
                WHEN obes.has_lower_bmi_threshold_ethnicity THEN 37.5
                ELSE 40
            END,
            FALSE
        ) AS bmi_meets_cohort_2
    FROM obesity2 AS obes
    INNER JOIN qualifying_bmi_evidence AS bmi
        ON obes.person_id = bmi.person_id
    INNER JOIN {{ ref('dim_person_current_practice') }} AS prac
        ON obes.person_id = prac.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON obes.person_id = age.person_id
    WHERE prac.registration_end_date IS NULL
      AND COALESCE(age.is_deceased, FALSE) = FALSE
)

SELECT
    person_id,
    age,
    latest_bmi_value,
    latest_bmi_date,
    latest_bmi_source_cluster_id,
    latest_bmi_is_bmi_35_code,
    CASE
        WHEN bmi_meets_cohort_1 THEN 'Obese Class III'
        WHEN bmi_meets_cohort_2 THEN 'Obese Class II'
        ELSE 'BMI assessment needed'
    END AS bmi_category,
    has_lower_bmi_threshold_ethnicity,
    has_unresolved_hypertension,
    has_dyslipidaemia,
    has_obstructive_sleep_apnoea,
    has_ascvd,
    has_unresolved_type2_diabetes,
    qualifying_comorbidity_count,
    bmi_meets_cohort_1,
    bmi_meets_cohort_2,
    bmi_meets_cohort_1 AS is_eligible_cohort_1,
    bmi_meets_cohort_2 AS is_eligible_cohort_2
FROM cohorted
