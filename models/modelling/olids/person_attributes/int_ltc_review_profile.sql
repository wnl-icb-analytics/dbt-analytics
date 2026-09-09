{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-person profile for the NICE long-term condition review indicators: smoking
(IND156, IND157), BMI recording (IND320), multimorbidity (IND207, IND208) and
alcohol (IND196 to IND202). One row per person in dim_person. Combines register
membership from fct_person_ltc_summary, frailty severity, and the latest
smoking, BMI, medication review, falls discussion, alcohol screening and brief
intervention records, so each measure applies only its own population and
window. No registration, living or test-patient filter; consumers join
dim_person_active_patients.

The smoking-status LTC list (IND156, IND157) is CHD, PAD, stroke/TIA,
hypertension, diabetes, COPD, CKD and asthma. Multimorbidity follows NICE IND205: four or
more condition clusters, mapped from the project registers (cancer; circulatory;
diabetes; digestive as chronic liver disease; learning disability; mental health
including alcohol problems; musculoskeletal as rheumatoid arthritis; neurological;
renal; respiratory). The chronic pain, constipation, diverticular disease,
inflammatory bowel disease, eating disorder and substance misuse conditions are
not modelled, so the count is conservative. Alcohol brief intervention counts
intervention records (advice, education, referral), not declined codes.
*/

WITH conditions AS (
    SELECT
        person_id,
        COUNT(DISTINCT condition_code) AS ltc_count,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'PAD') AS has_pad,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stroke_tia,
        BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
        BOOLOR_AGG(condition_code = 'DM') AS has_diabetes,
        BOOLOR_AGG(condition_code = 'NDH') AS has_ndh,
        BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
        BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
        BOOLOR_AGG(condition_code IN ('AST', 'CYP_AST')) AS has_asthma,
        BOOLOR_AGG(condition_code = 'AF') AS has_atrial_fibrillation,
        BOOLOR_AGG(condition_code = 'HF') AS has_heart_failure,
        BOOLOR_AGG(condition_code = 'DEM') AS has_dementia,
        BOOLOR_AGG(condition_code = 'DEP') AS has_depression,
        BOOLOR_AGG(condition_code = 'ANX') AS has_anxiety,
        BOOLOR_AGG(condition_code = 'SMI') AS has_smi,
        BOOLOR_AGG(condition_code IN ('LD', 'LD_U14')) AS has_learning_disability,
        COUNT(DISTINCT CASE
            WHEN condition_code = 'CAN' THEN 'CANCER'
            WHEN condition_code IN ('CHD', 'AF', 'HF', 'HTN', 'STIA', 'PAD') THEN 'CIRCULATORY'
            WHEN condition_code = 'DM' THEN 'DIABETES'
            WHEN condition_code = 'CLD' THEN 'DIGESTIVE'
            WHEN condition_code IN ('LD', 'LD_U14') THEN 'LEARNING_DISABILITY'
            WHEN condition_code IN ('ANX', 'DEP', 'DEM', 'SMI') THEN 'MENTAL_HEALTH'
            WHEN condition_code = 'RA' THEN 'MUSCULOSKELETAL'
            WHEN condition_code IN ('EP', 'MS', 'PD') THEN 'NEUROLOGICAL'
            WHEN condition_code = 'CKD' THEN 'RENAL'
            WHEN condition_code IN ('AST', 'CYP_AST', 'COPD') THEN 'RESPIRATORY'
        END) AS register_cluster_count,
        MIN(CASE WHEN condition_code IN ('CHD', 'PAD', 'STIA', 'HTN', 'DM', 'COPD', 'CKD', 'AST', 'CYP_AST')
            THEN earliest_diagnosis_date::DATE END) AS earliest_smoking_ltc_diagnosis_date,
        MIN(CASE WHEN condition_code = 'HTN' THEN earliest_diagnosis_date::DATE END) AS earliest_hypertension_date,
        MIN(CASE WHEN condition_code IN ('DEP', 'ANX') THEN earliest_diagnosis_date::DATE END) AS earliest_depression_anxiety_date
    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE is_on_register
    GROUP BY person_id
),

smoking AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_smoking_status_date,
        smoking_status AS latest_smoking_status
    FROM {{ ref('int_smoking_status_latest') }}
),

never_smoked AS (
    SELECT
        person_id,
        MAX(clinical_effective_date::DATE) AS latest_never_smoked_date
    FROM {{ ref('int_smoking_status_all') }}
    WHERE is_never_smoked_code
    GROUP BY person_id
),

smoking_intervention AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_smoking_intervention_date
    FROM {{ ref('int_smoking_intervention') }}
    GROUP BY person_id
),

bmi AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_bmi_date
    FROM {{ ref('int_bmi_all') }}
    WHERE bmi_value IS NOT NULL
    GROUP BY person_id
),

medication_review AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_structured_medication_review_date
    FROM {{ ref('int_structured_medication_review_all') }}
    GROUP BY person_id
),

falls AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_falls_discussion_date
    FROM {{ ref('int_falls_discussion_all') }}
    GROUP BY person_id
),

latest_screen AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_alcohol_screen_date,
        screening_tool AS latest_alcohol_screen_tool,
        score_value AS latest_alcohol_screen_score,
        is_positive_screen AS is_latest_alcohol_screen_positive
    FROM {{ ref('int_alcohol_screening_all') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

latest_positive AS (
    SELECT
        person_id,
        MAX(clinical_effective_date::DATE) AS latest_positive_alcohol_screen_date
    FROM {{ ref('int_alcohol_screening_all') }}
    WHERE is_positive_screen
    GROUP BY person_id
),

brief_intervention AS (
    SELECT person_id, clinical_effective_date::DATE AS intervention_date
    FROM {{ ref('int_alcohol_intervention') }}
    WHERE alcohol_advice_services = 'Yes'
),

intervention_after_positive AS (
    SELECT
        positive.person_id,
        MAX(bi.intervention_date) AS latest_intervention_after_positive_screen_date
    FROM latest_positive AS positive
    INNER JOIN brief_intervention AS bi
        ON positive.person_id = bi.person_id
        AND bi.intervention_date BETWEEN positive.latest_positive_alcohol_screen_date
            AND DATEADD(month, 3, positive.latest_positive_alcohol_screen_date)
    GROUP BY positive.person_id
),

alcohol_disorder AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_alcohol_misuse_disorders') }}
),

dyslipidaemia AS (
    SELECT DISTINCT person_id FROM {{ ref('int_dyslipidaemia_diagnoses_all') }}
),

sleep_apnoea AS (
    SELECT DISTINCT person_id FROM {{ ref('int_obstructive_sleep_apnoea_diagnoses_all') }}
)

SELECT
    person.person_id,
    age.birth_date_approx,
    COALESCE(c.ltc_count, 0) AS ltc_count,
    -- Alcohol problems form part of the mental health cluster
    COALESCE(c.register_cluster_count, 0)
        + IFF(alcohol_disorder.person_id IS NOT NULL
              AND NOT COALESCE(c.has_anxiety OR c.has_depression OR c.has_dementia OR c.has_smi, FALSE), 1, 0)
        AS multimorbidity_cluster_count,
    COALESCE(c.has_chd, FALSE) AS has_chd,
    COALESCE(c.has_pad, FALSE) AS has_pad,
    COALESCE(c.has_stroke_tia, FALSE) AS has_stroke_tia,
    COALESCE(c.has_hypertension, FALSE) AS has_hypertension,
    COALESCE(c.has_diabetes, FALSE) AS has_diabetes,
    COALESCE(c.has_ndh, FALSE) AS has_ndh,
    COALESCE(c.has_copd, FALSE) AS has_copd,
    COALESCE(c.has_ckd, FALSE) AS has_ckd,
    COALESCE(c.has_asthma, FALSE) AS has_asthma,
    COALESCE(c.has_atrial_fibrillation, FALSE) AS has_atrial_fibrillation,
    COALESCE(c.has_heart_failure, FALSE) AS has_heart_failure,
    COALESCE(c.has_dementia, FALSE) AS has_dementia,
    COALESCE(c.has_depression, FALSE) AS has_depression,
    COALESCE(c.has_anxiety, FALSE) AS has_anxiety,
    COALESCE(c.has_smi, FALSE) AS has_smi,
    COALESCE(c.has_learning_disability, FALSE) AS has_learning_disability,
    dyslipidaemia.person_id IS NOT NULL AS has_dyslipidaemia,
    sleep_apnoea.person_id IS NOT NULL AS has_obstructive_sleep_apnoea,
    c.earliest_smoking_ltc_diagnosis_date,
    c.earliest_hypertension_date,
    c.earliest_depression_anxiety_date,
    frailty.latest_frailty_severity,
    smoking.latest_smoking_status,
    smoking.latest_smoking_status_date,
    never_smoked.latest_never_smoked_date,
    smoking_intervention.latest_smoking_intervention_date,
    bmi.latest_bmi_date,
    medication_review.latest_structured_medication_review_date,
    falls.latest_falls_discussion_date,
    latest_screen.latest_alcohol_screen_date,
    latest_screen.latest_alcohol_screen_tool,
    latest_screen.latest_alcohol_screen_score,
    COALESCE(latest_screen.is_latest_alcohol_screen_positive, FALSE) AS is_latest_alcohol_screen_positive,
    latest_positive.latest_positive_alcohol_screen_date,
    intervention_after_positive.latest_intervention_after_positive_screen_date,
    alcohol_disorder.person_id IS NOT NULL AS has_alcohol_disorder
FROM {{ ref('dim_person') }} AS person
LEFT JOIN {{ ref('dim_person_age') }} AS age ON person.person_id = age.person_id
LEFT JOIN conditions AS c ON person.person_id = c.person_id
LEFT JOIN {{ ref('fct_person_frailty_register') }} AS frailty ON person.person_id = frailty.person_id
LEFT JOIN smoking ON person.person_id = smoking.person_id
LEFT JOIN never_smoked ON person.person_id = never_smoked.person_id
LEFT JOIN smoking_intervention ON person.person_id = smoking_intervention.person_id
LEFT JOIN bmi ON person.person_id = bmi.person_id
LEFT JOIN medication_review ON person.person_id = medication_review.person_id
LEFT JOIN falls ON person.person_id = falls.person_id
LEFT JOIN latest_screen ON person.person_id = latest_screen.person_id
LEFT JOIN latest_positive ON person.person_id = latest_positive.person_id
LEFT JOIN intervention_after_positive ON person.person_id = intervention_after_positive.person_id
LEFT JOIN alcohol_disorder ON person.person_id = alcohol_disorder.person_id
LEFT JOIN dyslipidaemia ON person.person_id = dyslipidaemia.person_id
LEFT JOIN sleep_apnoea ON person.person_id = sleep_apnoea.person_id
