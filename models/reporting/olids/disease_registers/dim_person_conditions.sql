{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Person Conditions Dimension
-- Wide format with explicit boolean flags for ALL persons from dim_person_demographics
-- Includes persons with no conditions (all flags = FALSE) for complete population view
-- One row per person matching dim_person_demographics grain for 1:1:1 joins

WITH all_persons AS (
    SELECT person_id 
    FROM {{ ref('dim_person_demographics') }}
),

diabetes_register AS (
    SELECT person_id, diabetes_type
    FROM {{ ref('fct_person_diabetes_register') }}
),

-- LTC rows with subset registers collapsed into their parent so condition counts
-- are not double-counted. LD_U14 is a subset of LD, and CYP_AST overlaps AST -
-- counting both would count the same underlying disease twice. Canonical QOF
-- status follows the parent register (both parents are QOF); clinical_domain is
-- shared between parent and child. Raw condition_code is retained for the flags.
condition_registers AS (
    SELECT
        person_id,
        condition_code,
        clinical_domain,
        is_qof,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        CASE
            WHEN condition_code = 'LD_U14' THEN 'LD'
            WHEN condition_code = 'CYP_AST' THEN 'AST'
            ELSE condition_code
        END AS canonical_condition_code,
        CASE
            WHEN condition_code IN ('LD_U14', 'CYP_AST') THEN TRUE
            ELSE is_qof
        END AS canonical_is_qof
    FROM {{ ref('fct_person_ltc_summary') }}
),

person_conditions AS (
    SELECT
        p.person_id,

        -- Diabetes type from register (Type 1, Type 2, Unknown, or Not Diabetic)
        COALESCE(dm_reg.diabetes_type, 'Not Diabetic') AS diabetes_type,

        -- Boolean condition flags (pivoted from condition_code) - explicit TRUE/FALSE for all
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'AF' THEN TRUE END), FALSE) AS has_atrial_fibrillation,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'AST' THEN TRUE END), FALSE) AS has_asthma,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CAN' THEN TRUE END), FALSE) AS has_cancer,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CHD' THEN TRUE END), FALSE) AS has_coronary_heart_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CKD' THEN TRUE END), FALSE) AS has_chronic_kidney_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'COPD' THEN TRUE END), FALSE) AS has_copd,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CYP_AST' THEN TRUE END), FALSE) AS has_cyp_asthma,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'DEM' THEN TRUE END), FALSE) AS has_dementia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'DEP' THEN TRUE END), FALSE) AS has_depression,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'DM' THEN TRUE END), FALSE) AS has_diabetes,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'EP' THEN TRUE END), FALSE) AS has_epilepsy,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'FH' THEN TRUE END), FALSE) AS has_familial_hypercholesterolaemia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'GESTDIAB' THEN TRUE END), FALSE) AS has_gestational_diabetes,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'FRAIL' THEN TRUE END), FALSE) AS has_frailty,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'HF' THEN TRUE END), FALSE) AS has_heart_failure,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'HTN' THEN TRUE END), FALSE) AS has_hypertension,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'LD' THEN TRUE END), FALSE) AS has_learning_disability,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'LD_U14' THEN TRUE END), FALSE) AS has_learning_disability_under_14,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'NAFLD' THEN TRUE END), FALSE) AS has_nafld,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'NDH' THEN TRUE END), FALSE) AS has_non_diabetic_hyperglycaemia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'OB' THEN TRUE END), FALSE) AS has_obesity,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'OST' THEN TRUE END), FALSE) AS has_osteoporosis,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'OA' THEN TRUE END), FALSE) AS has_osteoarthritis,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'PAD' THEN TRUE END), FALSE) AS has_peripheral_arterial_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'PC' THEN TRUE END), FALSE) AS has_palliative_care,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'RA' THEN TRUE END), FALSE) AS has_rheumatoid_arthritis,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'SMI' THEN TRUE END), FALSE) AS has_severe_mental_illness,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'STIA' THEN TRUE END), FALSE) AS has_stroke_tia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'PD' THEN TRUE END), FALSE) AS has_parkinsons,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CEREBRALP' THEN TRUE END), FALSE) AS has_cerebral_palsy,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'MND' THEN TRUE END), FALSE) AS has_mnd,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'MS' THEN TRUE END), FALSE) AS has_multiple_sclerosis,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'ANX' THEN TRUE END), FALSE) AS has_anxiety,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'THY' THEN TRUE END), FALSE) AS has_hypothyroidism,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'AUTISM' THEN TRUE END), FALSE) AS has_autism,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'ADHD' THEN TRUE END), FALSE) AS has_adhd,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CLD' THEN TRUE END), FALSE) AS has_chronic_liver_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'SCD' THEN TRUE END), FALSE) AS has_sickle_cell,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'THAL' THEN TRUE END), FALSE) AS has_thalassaemia,

        -- Summary counts (0 for persons with no conditions)
        -- Counts use canonical_condition_code so subset registers (LD_U14, CYP_AST)
        -- are not double-counted against their parent register.
        COALESCE(COUNT(DISTINCT ltc.canonical_condition_code), 0) AS total_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.canonical_is_qof = TRUE THEN ltc.canonical_condition_code END), 0) AS total_qof_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.canonical_is_qof = FALSE THEN ltc.canonical_condition_code END), 0) AS total_non_qof_conditions,

        -- Clinical domain counts (0 for persons with no conditions)
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Cardiovascular' THEN ltc.canonical_condition_code END), 0) AS cardiovascular_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Respiratory' THEN ltc.canonical_condition_code END), 0) AS respiratory_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Mental Health' THEN ltc.canonical_condition_code END), 0) AS mental_health_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Metabolic' THEN ltc.canonical_condition_code END), 0) AS metabolic_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Musculoskeletal' THEN ltc.canonical_condition_code END), 0) AS musculoskeletal_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Neurology' THEN ltc.canonical_condition_code END), 0) AS neurology_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Geriatric' THEN ltc.canonical_condition_code END), 0) AS geriatric_conditions,
        
        -- Earliest and latest diagnosis dates across all conditions (NULL for no conditions)
        MIN(ltc.earliest_diagnosis_date) AS earliest_condition_diagnosis,
        MAX(ltc.latest_diagnosis_date) AS latest_condition_diagnosis

    FROM all_persons p
    LEFT JOIN condition_registers ltc
        ON p.person_id = ltc.person_id
    LEFT JOIN diabetes_register dm_reg
        ON p.person_id = dm_reg.person_id
    GROUP BY p.person_id, dm_reg.diabetes_type
)

SELECT * FROM person_conditions