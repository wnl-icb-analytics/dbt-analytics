{{ config(
    materialized='table',
    tags=['fact', 'programme', 'tirzepatide', 'powerbi'],
    cluster_by=['person_id', 'practice_code']) }}

/*
Tirzepatide cohort status fact table (person-level, PowerBI-ready).

Combines the eligibility population (int_tirzepatide_eligible_population) with
current GLP-1 prescribing (int_glp1_medications_all) and demographic / practice
context, so practices can identify candidates for assessment and see
whether they are already on a GLP-1 (and for which likely indication).

Grain: one row per currently-registered, living OBES2 member.
*/

WITH eligible AS (
    SELECT *
    FROM {{ ref('int_tirzepatide_eligible_population') }}
),

-- Collapse order-level GLP-1 prescribing to one row per person
glp1_orders AS (
    SELECT
        person_id,
        order_date,
        glp1_drug,
        is_dual_gip_glp1,
        is_diabetes_indication,
        is_recent_6m,
        rolling_order_count_12m
    FROM {{ ref('int_glp1_medications_all') }}
),

-- Person-level prescribing summary (BOOLOR_AGG: Snowflake MAX rejects booleans)
glp1_agg AS (
    SELECT
        person_id,
        BOOLOR_AGG(is_dual_gip_glp1) AS ever_on_tirzepatide,
        MAX(order_date) AS latest_glp1_order_date,
        BOOLOR_AGG(is_recent_6m) AS is_currently_treated_glp1,
        MAX(rolling_order_count_12m) AS glp1_orders_12m
    FROM glp1_orders
    GROUP BY person_id
),

-- Attributes of the most recent order (drug, likely indication)
glp1_latest AS (
    SELECT
        person_id,
        glp1_drug AS latest_glp1_drug,
        is_dual_gip_glp1 AS latest_order_is_tirzepatide,
        CASE WHEN is_diabetes_indication THEN 'Diabetes' ELSE 'Obesity' END AS latest_glp1_indication
    FROM glp1_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY order_date DESC) = 1
),

glp1_person AS (
    SELECT
        a.person_id,
        TRUE AS ever_on_glp1,
        a.ever_on_tirzepatide,
        a.latest_glp1_order_date,
        a.is_currently_treated_glp1,
        a.glp1_orders_12m,
        l.latest_glp1_drug,
        l.latest_order_is_tirzepatide,
        l.latest_glp1_indication
    FROM glp1_agg AS a
    INNER JOIN glp1_latest AS l ON a.person_id = l.person_id
)

SELECT
    -- Core identifiers
    elig.person_id,
    demo.sk_patient_id,

    -- Practice context
    prac.practice_code,
    prac.practice_name,
    demo.pcn_code,
    demo.pcn_name,
    demo.borough_registered,
    demo.neighbourhood_registered,

    -- Demographics
    elig.age,
    demo.gender,
    demo.age_band_nhs,
    demo.ethnicity_category,
    demo.imd_quintile_19,

    -- Eligibility: cohort assignment
    elig.is_eligible_cohort_1,
    elig.is_eligible_cohort_2,
    CASE
        WHEN elig.is_eligible_cohort_1
            THEN 'Cohort 1 (BMI >= 40/37.5)'
        WHEN elig.is_eligible_cohort_2
            THEN 'Cohort 2 (BMI 35-<40/32.5-<37.5)'
        ELSE 'BMI assessment needed'
    END AS cohort,

    -- BMI
    elig.latest_bmi_value,
    elig.latest_bmi_date,
    elig.latest_bmi_source_cluster_id,
    elig.latest_bmi_is_bmi_35_code,
    elig.bmi_category,
    elig.has_lower_bmi_threshold_ethnicity,

    -- Qualifying comorbidities
    elig.has_unresolved_hypertension,
    elig.has_dyslipidaemia,
    elig.has_obstructive_sleep_apnoea,
    elig.has_ascvd,
    elig.has_unresolved_type2_diabetes,
    elig.qualifying_comorbidity_count,

    -- GLP-1 prescribing status
    COALESCE(glp1.ever_on_glp1, FALSE) AS ever_on_glp1,
    COALESCE(glp1.ever_on_tirzepatide, FALSE) AS ever_on_tirzepatide,
    COALESCE(glp1.is_currently_treated_glp1, FALSE) AS is_currently_treated_glp1,
    glp1.latest_glp1_drug,
    glp1.latest_glp1_indication,
    glp1.latest_glp1_order_date,
    glp1.glp1_orders_12m,

    -- Includes candidates needing BMI assessment before a cohort can be assigned.
    NOT COALESCE(glp1.is_currently_treated_glp1, FALSE) AS is_actionable,

    -- Metadata
    CURRENT_DATE() AS data_refresh_date

FROM eligible AS elig
INNER JOIN {{ ref('dim_person_demographics') }} AS demo
    ON elig.person_id = demo.person_id
INNER JOIN {{ ref('dim_person_current_practice') }} AS prac
    ON elig.person_id = prac.person_id
LEFT JOIN glp1_person AS glp1
    ON elig.person_id = glp1.person_id
WHERE prac.registration_end_date IS NULL  -- Current registrations only
ORDER BY elig.qualifying_comorbidity_count DESC, elig.person_id
