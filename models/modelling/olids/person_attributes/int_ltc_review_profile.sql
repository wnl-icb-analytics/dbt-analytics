{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-person profile for the NICE long-term condition review indicators: smoking
(IND97, IND156, IND157), BMI recording (IND320), multimorbidity (IND207, IND208),
alcohol (IND196 to IND202), the condition reviews (IND104, IND110, IND139,
IND142, IND191, IND195, IND223, IND265, IND266, IND273) and the severe mental
illness physical health checks (IND82 to IND87, IND143, IND154, IND155, IND158,
IND159, IND248). One row per person in dim_person. Combines register membership from fct_person_ltc_summary, frailty
severity, the latest smoking, BMI, medication review, falls discussion, alcohol
screening and brief intervention records, the latest condition review, care
plan, health check, MRC, NYHA and thyroid function test records, the latest new
depression diagnosis with its 10-to-35-day review, the first cancer care review
after the latest new cancer diagnosis, whether ethnicity is recorded, the
latest blood pressure, lipid, glucose or HbA1c and alcohol consumption
records, the SMI register detail (active diagnosis, lithium therapy, care
plan, lithium levels) and the earliest cardiovascular disease and diabetes
diagnoses, so each measure applies only its own population and window. No registration, living or test-patient filter; consumers join
dim_person_active_patients.

The smoking-status LTC list (IND156, IND157) is CHD, PAD, stroke/TIA,
hypertension, diabetes, COPD, CKD and asthma; IND97 adds severe mental illness. Multimorbidity follows NICE IND205: four or
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
        BOOLOR_AGG(condition_code = 'RA') AS has_rheumatoid_arthritis,
        BOOLOR_AGG(condition_code = 'THY') AS has_hypothyroidism,
        BOOLOR_AGG(condition_code = 'CAN') AS has_cancer,
        MAX(CASE WHEN condition_code = 'CAN' THEN latest_diagnosis_date::DATE END) AS latest_cancer_diagnosis_date,
        MIN(CASE WHEN condition_code = 'SMI' THEN earliest_diagnosis_date::DATE END) AS earliest_smi_diagnosis_date,
        MIN(CASE WHEN condition_code = 'DEM' THEN earliest_diagnosis_date::DATE END) AS earliest_dementia_diagnosis_date,
        MIN(CASE WHEN condition_code = 'DM' THEN earliest_diagnosis_date::DATE END) AS earliest_diabetes_diagnosis_date,
        COUNT(DISTINCT CASE
            WHEN condition_code = 'CAN' THEN 'CANCER'
            WHEN condition_code IN ('CHD', 'AF', 'HF', 'HTN', 'STIA', 'PAD') THEN 'CIRCULATORY'
            WHEN condition_code = 'DM' THEN 'DIABETES'
            WHEN condition_code = 'CLD' THEN 'DIGESTIVE'
            WHEN condition_code IN ('LD', 'LD_U14') THEN 'LEARNING_DISABILITY'
            WHEN condition_code IN ('ANX', 'DEP', 'DEM')
                OR (condition_code = 'SMI' AND earliest_diagnosis_date IS NOT NULL) THEN 'MENTAL_HEALTH'
            WHEN condition_code = 'RA' THEN 'MUSCULOSKELETAL'
            WHEN condition_code IN ('EP', 'MS', 'PD') THEN 'NEUROLOGICAL'
            WHEN condition_code = 'CKD' THEN 'RENAL'
            WHEN condition_code IN ('AST', 'CYP_AST', 'COPD') THEN 'RESPIRATORY'
        END) AS register_cluster_count,
        MIN(CASE WHEN condition_code IN ('CHD', 'PAD', 'STIA', 'HTN', 'DM', 'COPD', 'CKD', 'AST', 'CYP_AST')
            THEN earliest_diagnosis_date::DATE END) AS earliest_smoking_ltc_diagnosis_date,
        MIN(CASE WHEN condition_code IN ('CHD', 'PAD', 'STIA', 'HTN', 'DM', 'COPD', 'CKD', 'AST', 'CYP_AST', 'SMI')
            THEN earliest_diagnosis_date::DATE END) AS earliest_smoking_smi_ltc_diagnosis_date,
        MIN(CASE WHEN condition_code = 'HTN' THEN earliest_diagnosis_date::DATE END) AS earliest_hypertension_date,
        MIN(CASE WHEN condition_code IN ('DEP', 'ANX') THEN earliest_diagnosis_date::DATE END) AS earliest_depression_anxiety_date
    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE is_on_register
    GROUP BY person_id
),

child_depression AS (
    -- NICE IND198/199 includes ages 10 to 17, outside the adult depression register.
    SELECT
        diagnosis.person_id,
        MIN(CASE WHEN diagnosis.is_diagnosis_code AND diagnosis.is_first_or_new_episode
            THEN diagnosis.clinical_effective_date::DATE END) AS earliest_diagnosis_date
    FROM {{ ref('int_depression_diagnoses_all') }} AS diagnosis
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON diagnosis.person_id = age.person_id
        AND age.age BETWEEN 10 AND 17
    WHERE diagnosis.clinical_effective_date::DATE <= CURRENT_DATE()
    GROUP BY diagnosis.person_id
    -- Preserve the existing register's diagnosis and resolution ordering.
    HAVING MAX(CASE WHEN diagnosis.is_diagnosis_code AND diagnosis.is_first_or_new_episode
        THEN diagnosis.clinical_effective_date END)
        > COALESCE(MAX(CASE WHEN diagnosis.is_resolved_code
            THEN diagnosis.clinical_effective_date END), '1900-01-01')
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

smoking_pharmacotherapy_codes AS (
    -- QOF v51 PHARMDRUG_COD is the smoking pharmacotherapy drug refset.
    SELECT DISTINCT referenced_component_id::VARCHAR AS mapped_concept_code
    FROM {{ ref('stg_nhsd_snomed_sct_refset_simple') }}
    WHERE ref_set_id = 12465801000001106 AND active
),

smoking_support AS (
    -- QOF v51 SMOK004 defines support as referral or pharmacotherapy, including prescriptions.
    SELECT person_id, clinical_effective_date::DATE AS support_date
    FROM ({{ get_observations("'REFERSSSA_COD', 'PHARM_COD'", source='PCD') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()

    UNION ALL

    SELECT
        observation.person_id,
        -- Match the observation date correction used by get_observations.
        CASE WHEN observation.clinical_effective_date > observation.date_recorded
            THEN observation.date_recorded
            ELSE COALESCE(observation.clinical_effective_date, '1900-01-01')
        END::DATE AS support_date
    FROM {{ ref('stg_olids_observation') }} AS observation
    INNER JOIN smoking_pharmacotherapy_codes AS codes
        ON observation.mapped_concept_code = codes.mapped_concept_code
    WHERE support_date <= CURRENT_DATE()

    UNION ALL

    SELECT person.person_id, medication.clinical_effective_date::DATE AS support_date
    FROM {{ ref('stg_olids_medication_order') }} AS medication
    INNER JOIN {{ ref('int_patient_person_unique') }} AS person
        ON medication.patient_id = person.patient_id
    INNER JOIN smoking_pharmacotherapy_codes AS codes
        ON medication.mapped_concept_code = codes.mapped_concept_code
    WHERE medication.clinical_effective_date::DATE <= CURRENT_DATE()
),

smoking_intervention AS (
    SELECT person_id, MAX(support_date) AS latest_smoking_intervention_date
    FROM smoking_support
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
        AND screening_tool IN ('FAST', 'AUDIT-C')
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

reviews AS (
    SELECT
        person_id,
        MAX(CASE WHEN review_type = 'ASTHMA_REVIEW' THEN clinical_effective_date::DATE END) AS latest_asthma_review_date,
        MAX(CASE WHEN review_type = 'COPD_REVIEW' THEN clinical_effective_date::DATE END) AS latest_copd_review_date,
        MAX(CASE WHEN review_type = 'COPD_EXACERBATION_COUNT' THEN clinical_effective_date::DATE END) AS latest_copd_exacerbation_count_date,
        MAX(CASE WHEN review_type = 'HEART_FAILURE_REVIEW' THEN clinical_effective_date::DATE END) AS latest_heart_failure_review_date,
        MAX(CASE WHEN review_type IN ('MEDICATION_REVIEW', 'HEART_FAILURE_MEDICATION_REVIEW')
            THEN clinical_effective_date::DATE END) AS latest_coded_medication_review_date,
        MAX(CASE WHEN review_type = 'RHEUMATOID_ARTHRITIS_REVIEW' THEN clinical_effective_date::DATE END) AS latest_rheumatoid_arthritis_review_date,
        MAX(CASE WHEN review_type = 'LEARNING_DISABILITY_HEALTH_CHECK' THEN clinical_effective_date::DATE END) AS latest_ld_health_check_date,
        MAX(CASE WHEN review_type = 'LEARNING_DISABILITY_HEALTH_ACTION_PLAN' THEN clinical_effective_date::DATE END) AS latest_ld_health_action_plan_date,
        MAX(CASE WHEN review_type IN ('DEMENTIA_CARE_PLAN', 'DEMENTIA_CARE_PLAN_REVIEW')
            THEN clinical_effective_date::DATE END) AS latest_dementia_care_plan_date
    FROM {{ ref('int_ltc_review_all') }}
    GROUP BY person_id
),

asthma_review_dates AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS record_date,
        BOOLOR_AGG(review_type = 'ASTHMA_REVIEW') AS has_review,
        BOOLOR_AGG(review_type = 'ASTHMA_ACTION_PLAN') AS has_action_plan,
        BOOLOR_AGG(review_type = 'ASTHMA_EXACERBATION_COUNT') AS has_exacerbation_count
    FROM {{ ref('int_ltc_review_all') }}
    WHERE review_type IN ('ASTHMA_REVIEW', 'ASTHMA_ACTION_PLAN', 'ASTHMA_EXACERBATION_COUNT')
    GROUP BY person_id, clinical_effective_date::DATE
),

complete_asthma_review AS (
    -- QOF v51 AST015 requires a same-day plan and an exacerbation count in the preceding month.
    SELECT
        review.person_id,
        MAX(review.record_date) AS latest_complete_asthma_review_date
    FROM asthma_review_dates AS review
    INNER JOIN asthma_review_dates AS exacerbations
        ON review.person_id = exacerbations.person_id
        AND exacerbations.has_exacerbation_count
        AND exacerbations.record_date > DATEADD(month, -1, review.record_date)
        AND exacerbations.record_date <= review.record_date
    WHERE review.has_review AND review.has_action_plan
    GROUP BY review.person_id
),

mrc AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_mrc_dyspnoea_date
    FROM {{ ref('int_mrc_dyspnoea_all') }}
    GROUP BY person_id
),

nyha AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_nyha_date
    FROM {{ ref('int_nyha_classification_all') }}
    GROUP BY person_id
),

thyroid AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_thyroid_function_test_date
    FROM {{ ref('int_thyroid_function_test_all') }}
    GROUP BY person_id
),

cancer_review AS (
    -- First cancer care review on or after the latest first-or-new-episode cancer diagnosis
    SELECT
        c.person_id,
        MIN(review.clinical_effective_date::DATE) AS first_cancer_care_review_after_diagnosis_date
    FROM conditions AS c
    INNER JOIN {{ ref('int_ltc_review_all') }} AS review
        ON c.person_id = review.person_id
        AND review.review_type = 'CANCER_CARE_REVIEW'
        AND review.clinical_effective_date::DATE >= c.latest_cancer_diagnosis_date
    GROUP BY c.person_id
),

new_depression AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_new_depression_diagnosis_date
    FROM {{ ref('int_depression_diagnoses_all') }}
    WHERE is_diagnosis_code AND is_first_or_new_episode
        AND clinical_effective_date <= CURRENT_DATE()
    GROUP BY person_id
),

depression_review AS (
    -- First depression review 10 to 35 days after the latest new diagnosis
    SELECT
        dx.person_id,
        MIN(review.clinical_effective_date::DATE) AS first_depression_review_10_to_35_days_date
    FROM new_depression AS dx
    INNER JOIN {{ ref('int_ltc_review_all') }} AS review
        ON dx.person_id = review.person_id
        AND review.review_type = 'DEPRESSION_REVIEW'
        AND review.clinical_effective_date::DATE BETWEEN DATEADD(day, 10, dx.latest_new_depression_diagnosis_date)
            AND DATEADD(day, 35, dx.latest_new_depression_diagnosis_date)
    GROUP BY dx.person_id
),

ethnicity AS (
    SELECT person_id, ethnicity_category
    FROM {{ ref('dim_person_demographics') }}
),

blood_pressure AS (
    SELECT person_id, clinical_effective_date::DATE AS latest_blood_pressure_date
    FROM {{ ref('int_blood_pressure_latest') }}
),

cholesterol AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_total_cholesterol_date
    FROM {{ ref('int_cholesterol_all') }}
    WHERE cholesterol_value IS NOT NULL
    GROUP BY person_id
),

cholesterol_hdl_ratio AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_cholesterol_hdl_ratio_date
    FROM {{ ref('int_cholesterol_hdl_ratio_all') }}
    WHERE cholesterol_hdl_ratio IS NOT NULL
    GROUP BY person_id
),

hba1c AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_hba1c_date
    FROM {{ ref('int_hba1c_all') }}
    WHERE hba1c_original_value IS NOT NULL
    GROUP BY person_id
),

blood_glucose AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_blood_glucose_date
    FROM {{ ref('int_blood_glucose_all') }}
    WHERE result_value IS NOT NULL
    GROUP BY person_id
),

alcohol_units AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_alcohol_units_date
    FROM {{ ref('int_alcohol_units_all') }}
    GROUP BY person_id
),

alcohol_usage AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_alcohol_usage_date
    FROM {{ ref('int_alcohol_usage_all') }}
    GROUP BY person_id
),

smi_register AS (
    SELECT person_id, has_active_smi_diagnosis, is_on_lithium, latest_diagnosis_date::DATE AS latest_smi_diagnosis_date,
        latest_resolved_date::DATE AS latest_smi_remission_date
    FROM {{ ref('fct_person_smi_register') }}
    WHERE is_on_register
),

smi_care_plan AS (
    SELECT person_id, MAX(clinical_effective_date::DATE) AS latest_smi_care_plan_date
    FROM {{ ref('int_smi_care_plan_all') }}
    GROUP BY person_id
),

lithium_level AS (
    -- Latest serum lithium record, whether or not it carries a value
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_lithium_level_date,
        lithium_level AS latest_lithium_level,
        is_in_therapeutic_range AS is_latest_lithium_level_in_range
    FROM {{ ref('int_lithium_level_all') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY clinical_effective_date DESC, is_result_recorded DESC, id DESC
    ) = 1
),

cvd AS (
    SELECT person_id, earliest_cvd_diagnosis_date
    FROM {{ ref('int_cvd_secondary_prevention_population') }}
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
              AND NOT COALESCE(c.has_anxiety OR c.has_depression OR c.has_dementia
                  OR c.earliest_smi_diagnosis_date IS NOT NULL, FALSE), 1, 0)
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
    COALESCE(c.has_rheumatoid_arthritis, FALSE) AS has_rheumatoid_arthritis,
    COALESCE(c.has_hypothyroidism, FALSE) AS has_hypothyroidism,
    COALESCE(c.has_cancer, FALSE) AS has_cancer,
    c.latest_cancer_diagnosis_date,
    c.earliest_smi_diagnosis_date,
    c.earliest_dementia_diagnosis_date,
    c.earliest_diabetes_diagnosis_date,
    cvd.earliest_cvd_diagnosis_date,
    COALESCE(smi_register.has_active_smi_diagnosis, FALSE) AS has_active_smi_diagnosis,
    COALESCE(smi_register.is_on_lithium, FALSE) AS is_on_lithium,
    smi_register.latest_smi_diagnosis_date,
    smi_register.latest_smi_remission_date,
    dyslipidaemia.person_id IS NOT NULL AS has_dyslipidaemia,
    sleep_apnoea.person_id IS NOT NULL AS has_obstructive_sleep_apnoea,
    c.earliest_smoking_ltc_diagnosis_date,
    c.earliest_smoking_smi_ltc_diagnosis_date,
    c.earliest_hypertension_date,
    LEAST_IGNORE_NULLS(c.earliest_depression_anxiety_date,
        child_depression.earliest_diagnosis_date) AS earliest_depression_anxiety_date,
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
    alcohol_disorder.person_id IS NOT NULL AS has_alcohol_disorder,
    reviews.latest_asthma_review_date,
    complete_asthma_review.latest_complete_asthma_review_date,
    reviews.latest_copd_review_date,
    reviews.latest_copd_exacerbation_count_date,
    mrc.latest_mrc_dyspnoea_date,
    reviews.latest_heart_failure_review_date,
    nyha.latest_nyha_date,
    -- Any coded medication review, heart failure medication review or structured medication review
    GREATEST_IGNORE_NULLS(reviews.latest_coded_medication_review_date,
        medication_review.latest_structured_medication_review_date) AS latest_medication_review_date,
    reviews.latest_rheumatoid_arthritis_review_date,
    thyroid.latest_thyroid_function_test_date,
    reviews.latest_ld_health_check_date,
    reviews.latest_ld_health_action_plan_date,
    reviews.latest_dementia_care_plan_date,
    cancer_review.first_cancer_care_review_after_diagnosis_date,
    new_depression.latest_new_depression_diagnosis_date,
    depression_review.first_depression_review_10_to_35_days_date,
    COALESCE(ethnicity.ethnicity_category NOT IN ('Unknown'), FALSE) AS has_ethnicity_recorded,
    blood_pressure.latest_blood_pressure_date,
    cholesterol.latest_total_cholesterol_date,
    cholesterol_hdl_ratio.latest_cholesterol_hdl_ratio_date,
    -- Any lipid record: total cholesterol or total cholesterol:HDL ratio
    GREATEST_IGNORE_NULLS(cholesterol.latest_total_cholesterol_date,
        cholesterol_hdl_ratio.latest_cholesterol_hdl_ratio_date) AS latest_lipid_date,
    GREATEST_IGNORE_NULLS(hba1c.latest_hba1c_date, blood_glucose.latest_blood_glucose_date) AS latest_glucose_or_hba1c_date,
    -- Any alcohol consumption record: units per week, usage status or a screening tool
    GREATEST_IGNORE_NULLS(alcohol_units.latest_alcohol_units_date, alcohol_usage.latest_alcohol_usage_date,
        latest_screen.latest_alcohol_screen_date) AS latest_alcohol_record_date,
    smi_care_plan.latest_smi_care_plan_date,
    lithium_level.latest_lithium_level_date,
    lithium_level.latest_lithium_level,
    COALESCE(lithium_level.is_latest_lithium_level_in_range, FALSE) AS is_latest_lithium_level_in_range
FROM {{ ref('dim_person') }} AS person
LEFT JOIN {{ ref('dim_person_age') }} AS age ON person.person_id = age.person_id
LEFT JOIN conditions AS c ON person.person_id = c.person_id
LEFT JOIN child_depression ON person.person_id = child_depression.person_id
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
LEFT JOIN reviews ON person.person_id = reviews.person_id
LEFT JOIN complete_asthma_review ON person.person_id = complete_asthma_review.person_id
LEFT JOIN mrc ON person.person_id = mrc.person_id
LEFT JOIN nyha ON person.person_id = nyha.person_id
LEFT JOIN thyroid ON person.person_id = thyroid.person_id
LEFT JOIN cancer_review ON person.person_id = cancer_review.person_id
LEFT JOIN new_depression ON person.person_id = new_depression.person_id
LEFT JOIN depression_review ON person.person_id = depression_review.person_id
LEFT JOIN ethnicity ON person.person_id = ethnicity.person_id
LEFT JOIN blood_pressure ON person.person_id = blood_pressure.person_id
LEFT JOIN cholesterol ON person.person_id = cholesterol.person_id
LEFT JOIN cholesterol_hdl_ratio ON person.person_id = cholesterol_hdl_ratio.person_id
LEFT JOIN hba1c ON person.person_id = hba1c.person_id
LEFT JOIN blood_glucose ON person.person_id = blood_glucose.person_id
LEFT JOIN alcohol_units ON person.person_id = alcohol_units.person_id
LEFT JOIN alcohol_usage ON person.person_id = alcohol_usage.person_id
LEFT JOIN smi_register ON person.person_id = smi_register.person_id
LEFT JOIN smi_care_plan ON person.person_id = smi_care_plan.person_id
LEFT JOIN lithium_level ON person.person_id = lithium_level.person_id
LEFT JOIN cvd ON person.person_id = cvd.person_id
