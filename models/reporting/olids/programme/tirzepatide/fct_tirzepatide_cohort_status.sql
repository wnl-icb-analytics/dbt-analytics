{{ config(
    materialized='table',
    tags=['fact', 'programme', 'tirzepatide', 'powerbi'],
    cluster_by=['person_id', 'practice_code']) }}

/*
Tirzepatide cohort status fact table (person-level, PowerBI-ready).

Combines the candidate population (int_tirzepatide_candidates) with
current GLP-1 prescribing (int_glp1_medications_all) and demographic / practice
context, so practices can identify candidates for assessment and see
whether they are already on a GLP-1 (and for which likely indication).

Grain: one row per active OBES2 candidate.
*/

WITH candidates AS (
    SELECT *
    FROM {{ ref('int_tirzepatide_candidates') }}
),

-- Candidates' GLP-1 orders only, to keep the window and aggregate small
glp1_orders AS (
    SELECT
        glp1.person_id,
        glp1.medication_order_id,
        glp1.order_date,
        glp1.glp1_drug,
        glp1.is_dual_gip_glp1,
        glp1.bnf_code,
        glp1.is_recent_6m,
        glp1.is_recent_12m
    FROM {{ ref('int_glp1_medications_all') }} AS glp1
    INNER JOIN candidates AS cand
        ON glp1.person_id = cand.person_id
    -- Upstream recency flags have no upper bound; future-dated orders are not treatment
    WHERE glp1.order_date <= CURRENT_DATE()
),

-- Person-level prescribing summary (BOOLOR_AGG: Snowflake MAX rejects booleans)
glp1_agg AS (
    SELECT
        person_id,
        BOOLOR_AGG(is_dual_gip_glp1) AS has_tirzepatide_order,
        MAX(order_date) AS latest_glp1_order_date,
        BOOLOR_AGG(is_recent_6m) AS is_currently_treated_glp1,
        COUNT_IF(is_recent_12m) AS glp1_orders_12m
    FROM glp1_orders
    GROUP BY person_id
),

-- Attributes of the most recent order (drug, likely indication)
glp1_latest AS (
    SELECT
        person_id,
        glp1_drug AS latest_glp1_drug,
        -- BNF 6.1 = diabetes drugs, 4.5 = obesity drugs
        CASE
            WHEN bnf_code LIKE '0601%' THEN 'Diabetes'
            WHEN bnf_code LIKE '0405%' THEN 'Obesity'
            ELSE 'Unknown'
        END AS latest_glp1_indication
    FROM glp1_orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
),

glp1_person AS (
    SELECT
        a.person_id,
        a.has_tirzepatide_order,
        a.latest_glp1_order_date,
        a.is_currently_treated_glp1,
        a.glp1_orders_12m,
        l.latest_glp1_drug,
        l.latest_glp1_indication
    FROM glp1_agg AS a
    INNER JOIN glp1_latest AS l ON a.person_id = l.person_id
)

SELECT
    -- Core identifiers
    elig.person_id,
    demo.sk_patient_id,

    -- Practice context
    demo.practice_code,
    demo.practice_name,
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

    -- Cohort assignment
    elig.is_cohort_1,
    elig.is_cohort_2,
    CASE
        WHEN elig.is_cohort_1
            THEN 'Cohort 1 (BMI >= 40/37.5)'
        WHEN elig.is_cohort_2
            THEN 'Cohort 2 (BMI 35-<40/32.5-<37.5)'
        ELSE 'BMI assessment needed'
    END AS cohort,

    -- BMI from int_bmi_latest, mapped to programme cohorts upstream
    elig.latest_bmi_value,
    elig.latest_bmi_date,
    elig.latest_bmi_source_cluster_id,
    elig.bmi_category,
    elig.requires_lower_bmi_thresholds,
    elig.has_lower_bmi_threshold_ethnicity,

    -- Qualifying comorbidities
    elig.has_unresolved_hypertension,
    elig.has_dyslipidaemia,
    elig.has_obstructive_sleep_apnoea,
    elig.has_ascvd,
    elig.has_unresolved_type2_diabetes,
    elig.qualifying_comorbidity_count,

    -- GLP-1 prescribing status
    glp1.person_id IS NOT NULL AS has_glp1_order,
    COALESCE(glp1.has_tirzepatide_order, FALSE) AS has_tirzepatide_order,
    COALESCE(glp1.is_currently_treated_glp1, FALSE) AS is_currently_treated_glp1,
    glp1.latest_glp1_drug,
    glp1.latest_glp1_indication,
    glp1.latest_glp1_order_date,
    COALESCE(glp1.glp1_orders_12m, 0) AS glp1_orders_12m,

    -- Includes candidates needing BMI assessment before a cohort can be assigned.
    NOT COALESCE(glp1.is_currently_treated_glp1, FALSE) AS is_actionable,

    -- Metadata
    CURRENT_DATE() AS data_refresh_date

FROM candidates AS elig
INNER JOIN {{ ref('dim_person_demographics') }} AS demo
    ON elig.person_id = demo.person_id
LEFT JOIN glp1_person AS glp1
    ON elig.person_id = glp1.person_id
