{{
    config(
        materialized='table',
        tags=['intermediate', 'programme', 'tirzepatide'],
        cluster_by=['person_id'])
}}

/*
One row per active adult candidate in QOF v51 OBES2.
Membership and comorbidities come from fct_person_obesity2_register.
Numeric BMI and NICE NG246 class come from int_bmi_latest.
The programme maps Obese Class III to Cohort 1 and Obese Class II to Cohort 2
when that latest valid BMI is in the preceding 12 months.
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

latest_numeric_bmi AS (
    SELECT
        person_id,
        bmi_value,
        clinical_effective_date,
        source_cluster_id,
        requires_lower_bmi_thresholds,
        bmi_category
    FROM {{ ref('int_bmi_latest') }}
    WHERE clinical_effective_date > DATEADD(month, -12, CURRENT_DATE())
),

cohorted AS (
    SELECT
        obes.person_id,
        obes.age,
        bmi.bmi_value AS latest_bmi_value,
        bmi.clinical_effective_date AS latest_bmi_date,
        bmi.source_cluster_id AS latest_bmi_source_cluster_id,
        bmi.requires_lower_bmi_thresholds,
        obes.has_lower_bmi_threshold_ethnicity,
        obes.has_unresolved_hypertension,
        obes.has_dyslipidaemia,
        obes.has_obstructive_sleep_apnoea,
        obes.has_ascvd,
        obes.has_unresolved_type2_diabetes,
        obes.comorbidity_count AS qualifying_comorbidity_count,
        COALESCE(bmi.bmi_category = 'Obese Class III', FALSE) AS is_cohort_1,
        COALESCE(bmi.bmi_category = 'Obese Class II', FALSE) AS is_cohort_2
    FROM obesity2 AS obes
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON obes.person_id = active.person_id
    LEFT JOIN latest_numeric_bmi AS bmi
        ON obes.person_id = bmi.person_id
)

SELECT
    person_id,
    age,
    latest_bmi_value,
    latest_bmi_date,
    latest_bmi_source_cluster_id,
    CASE
        WHEN is_cohort_1 THEN 'Obese Class III'
        WHEN is_cohort_2 THEN 'Obese Class II'
        ELSE 'BMI assessment needed'
    END AS bmi_category,
    requires_lower_bmi_thresholds,
    has_lower_bmi_threshold_ethnicity,
    has_unresolved_hypertension,
    has_dyslipidaemia,
    has_obstructive_sleep_apnoea,
    has_ascvd,
    has_unresolved_type2_diabetes,
    qualifying_comorbidity_count,
    is_cohort_1,
    is_cohort_2
FROM cohorted
