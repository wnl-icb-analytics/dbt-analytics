{{
    config(
        materialized='table',
        cluster_by=['person_id', 'condition_code'])
}}

-- LTC Summary Fact Table
-- Comprehensive summary of all long-term condition registers.
-- Condition metadata (name/domain/qof) is sourced from ltc_register_denominator_rules
-- to keep register categorisation centralized.

WITH condition_union AS (
    -- Atrial Fibrillation
    SELECT
        person_id,
        'AF' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_atrial_fibrillation_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Asthma
    SELECT
        person_id,
        'AST' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_asthma_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Cancer
    SELECT
        person_id,
        'CAN' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_cancer_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Coronary Heart Disease
    SELECT
        person_id,
        'CHD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Chronic Kidney Disease
    SELECT
        person_id,
        'CKD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_ckd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- COPD
    SELECT
        person_id,
        'COPD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_copd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Children and Young People Asthma
    SELECT
        person_id,
        'CYP_AST' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_cyp_asthma_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Dementia
    SELECT
        person_id,
        'DEM' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_dementia_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Depression
    SELECT
        person_id,
        'DEP' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_depression_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Diabetes
    SELECT
        person_id,
        'DM' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Epilepsy
    SELECT
        person_id,
        'EP' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_epilepsy_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Familial Hypercholesterolaemia
    SELECT
        person_id,
        'FH' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_familial_hypercholesterolaemia_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Heart Failure
    SELECT
        person_id,
        'HF' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_heart_failure_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Hypertension
    SELECT
        person_id,
        'HTN' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_hypertension_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Learning Disability
    SELECT
        person_id,
        'LD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_learning_disability_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Learning Disability (Under 14)
    SELECT
        person_id,
        'LD_U14' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_learning_disability_register_under_14') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- NAFLD
    SELECT
        person_id,
        'NAFLD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_nafld_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Non-Diabetic Hyperglycaemia
    SELECT
        person_id,
        'NDH' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_ndh_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Obesity
    SELECT
        person_id,
        'OB' AS condition_code,
        is_on_register,
        latest_valid_bmi_date AS earliest_diagnosis_date,
        latest_bmi_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_obesity_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Osteoporosis
    SELECT
        person_id,
        'OST' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_osteoporosis_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Osteoarthritis
    SELECT
        person_id,
        'OA' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_osteoarthritis_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Peripheral Arterial Disease
    SELECT
        person_id,
        'PAD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_pad_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Palliative Care
    SELECT
        person_id,
        'PC' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_palliative_care_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Rheumatoid Arthritis
    SELECT
        person_id,
        'RA' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_rheumatoid_arthritis_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Severe Mental Illness
    SELECT
        person_id,
        'SMI' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_smi_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Stroke and TIA
    SELECT
        person_id,
        'STIA' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Gestational Diabetes
    SELECT
        person_id,
        'GESTDIAB' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_gestational_diabetes_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Frailty
    SELECT
        person_id,
        'FRAIL' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_frailty_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Parkinson's Disease
    SELECT
        person_id,
        'PD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_parkinsons_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Cerebral Palsy
    SELECT
        person_id,
        'CEREBRALP' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_cerebral_palsy_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Motor Neurone Disease
    SELECT
        person_id,
        'MND' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_mnd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Multiple Sclerosis
    SELECT
        person_id,
        'MS' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_ms_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Anxiety
    SELECT
        person_id,
        'ANX' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_anxiety_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Hypothyroidism
    SELECT
        person_id,
        'THY' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_hypothyroidism_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Autism Spectrum Disorder
    SELECT
        person_id,
        'AUTISM' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_autism_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- ADHD
    SELECT
        person_id,
        'ADHD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_adhd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Chronic Liver Disease
    SELECT
        person_id,
        'CLD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_chronic_liver_disease_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Sickle Cell Disease
    SELECT
        person_id,
        'SCD' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_sickle_cell_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Thalassaemia
    SELECT
        person_id,
        'THAL' AS condition_code,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_thalassaemia_register') }}
    WHERE is_on_register = TRUE
),

condition_metadata AS (
    SELECT
        UPPER(condition_code) AS condition_code,
        condition_name,
        clinical_domain,
        CAST(is_qof AS BOOLEAN) AS is_qof
    FROM {{ ref('ltc_register_denominator_rules') }}
)

SELECT
    cu.person_id,
    cu.condition_code,
    cm.condition_name,
    cm.clinical_domain,
    cu.is_on_register,
    cm.is_qof,
    cu.earliest_diagnosis_date,
    cu.latest_diagnosis_date
FROM condition_union AS cu
INNER JOIN condition_metadata AS cm
    ON cu.condition_code = cm.condition_code
ORDER BY cu.person_id, cu.condition_code
