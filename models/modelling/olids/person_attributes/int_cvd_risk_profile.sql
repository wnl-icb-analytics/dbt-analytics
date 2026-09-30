{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-person cardiovascular risk profile for the NICE primary-prevention and risk
assessment indicators (IND229, IND274, IND275, IND287, IND269, IND270, IND181,
IND161). One row per person in dim_person. Combines the QRISK history, risk
assessment records and the register, frailty and lipid-therapy facts those
indicators use as denominators and exclusions, so each measure applies only its
own window and age rule.

Established CVD follows the NICE definition: CHD or PAD register, or stroke/TIA
register without a history of haemorrhagic stroke for IND269 and IND270. The
separate all-stroke flag includes haemorrhagic stroke for primary-prevention
exclusions. No registration, living or
test-patient filter; consumers join dim_person_active_patients.
*/

WITH latest_score AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_risk_score_date,
        qrisk_score AS latest_risk_score,
        qrisk_type AS latest_risk_score_type
    FROM {{ ref('int_qrisk_all') }}
    WHERE is_valid_qrisk
        AND clinical_effective_date <= CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

score_history AS (
    SELECT
        person_id,
        MAX(qrisk_score) AS max_risk_score_ever,
        MAX(CASE WHEN clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
            THEN qrisk_score END) AS max_risk_score_12m,
        MIN(CASE WHEN clinical_effective_date::DATE >= DATEADD(month, -36, CURRENT_DATE())
            THEN qrisk_score END) AS min_risk_score_36m
    FROM {{ ref('int_qrisk_all') }}
    WHERE is_valid_qrisk
        AND clinical_effective_date <= CURRENT_DATE()
    GROUP BY person_id
),

latest_cvd_score AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_cvd_risk_score_date,
        CASE WHEN risk_score_value BETWEEN 0 AND 100 THEN risk_score_value END AS latest_cvd_risk_score
    FROM {{ ref('int_cvd_risk_assessment_all') }}
    WHERE is_cvd_risk_score_code
        AND original_result_value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

assessment_history AS (
    SELECT
        person_id,
        MAX(clinical_effective_date::DATE) AS latest_risk_assessment_date,
        MAX(CASE WHEN is_cvd_risk_score_code AND risk_score_value BETWEEN 0 AND 100
            AND clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
            THEN risk_score_value END) AS max_cvd_risk_score_12m,
        MAX(CASE WHEN is_cvd_risk_score_code AND risk_score_value >= 0 AND risk_score_value < 10
            AND clinical_effective_date::DATE >= DATEADD(month, -36, CURRENT_DATE())
            THEN clinical_effective_date::DATE END) AS latest_low_cvd_risk_score_date_36m,
        MAX(CASE WHEN is_cvd_risk_score_code AND risk_score_value BETWEEN 10 AND 100
            AND clinical_effective_date::DATE >= DATEADD(month, -36, CURRENT_DATE())
            THEN clinical_effective_date::DATE END) AS latest_high_cvd_risk_score_date_36m
    FROM {{ ref('int_cvd_risk_assessment_all') }}
    GROUP BY person_id
)

SELECT
    person.person_id,
    score.latest_risk_score,
    score.latest_risk_score_date,
    score.latest_risk_score_type,
    history.max_risk_score_ever,
    history.max_risk_score_12m,
    history.min_risk_score_36m,
    assessment.latest_risk_assessment_date,
    cvd_score.latest_cvd_risk_score,
    cvd_score.latest_cvd_risk_score_date,
    assessment.max_cvd_risk_score_12m,
    assessment.latest_low_cvd_risk_score_date_36m,
    assessment.latest_high_cvd_risk_score_date_36m,
    COALESCE(
        cvd.has_chd OR cvd.has_pad
        OR (cvd.has_stroke_tia AND NOT cvd.has_haemorrhagic_stroke),
        FALSE
    ) AS has_cvd,
    COALESCE(cvd.has_chd OR cvd.has_pad OR cvd.has_stroke_tia, FALSE) AS has_cvd_including_haemorrhagic_stroke,
    COALESCE(fh.is_on_register, FALSE) AS has_familial_hypercholesterolaemia,
    COALESCE(ckd.is_on_register, FALSE) AS has_ckd,
    COALESCE(diabetes.is_on_register, FALSE) AS has_diabetes,
    COALESCE(diabetes.diabetes_type = 'Type 1', FALSE) AS has_type1_diabetes,
    COALESCE(diabetes.diabetes_type = 'Type 2', FALSE) AS has_type2_diabetes,
    diabetes.earliest_type2_date::DATE AS earliest_type2_diabetes_date,
    COALESCE(hypertension.is_on_register, FALSE) AS has_hypertension,
    hypertension.earliest_diagnosis_date::DATE AS earliest_hypertension_date,
    frailty.latest_frailty_severity,
    therapy.latest_order_date AS latest_lipid_lowering_order_date,
    therapy.latest_statin_order_date,
    COALESCE(smoking.smoking_status = 'Current Smoker', FALSE) AS is_current_smoker,
    COALESCE(obesity.is_on_register, FALSE) AS has_obesity,
    cholesterol.cholesterol_value AS latest_total_cholesterol,
    cholesterol.clinical_effective_date::DATE AS latest_total_cholesterol_date
FROM {{ ref('dim_person') }} AS person
LEFT JOIN latest_score AS score ON person.person_id = score.person_id
LEFT JOIN score_history AS history ON person.person_id = history.person_id
LEFT JOIN assessment_history AS assessment ON person.person_id = assessment.person_id
LEFT JOIN latest_cvd_score AS cvd_score ON person.person_id = cvd_score.person_id
LEFT JOIN {{ ref('int_cvd_secondary_prevention_population') }} AS cvd ON person.person_id = cvd.person_id
LEFT JOIN {{ ref('fct_person_familial_hypercholesterolaemia_register') }} AS fh ON person.person_id = fh.person_id
LEFT JOIN {{ ref('fct_person_ckd_register') }} AS ckd ON person.person_id = ckd.person_id
LEFT JOIN {{ ref('fct_person_diabetes_register') }} AS diabetes ON person.person_id = diabetes.person_id
LEFT JOIN {{ ref('fct_person_hypertension_register') }} AS hypertension ON person.person_id = hypertension.person_id
LEFT JOIN {{ ref('fct_person_frailty_register') }} AS frailty ON person.person_id = frailty.person_id
LEFT JOIN {{ ref('int_lipid_lowering_therapy_latest') }} AS therapy ON person.person_id = therapy.person_id
LEFT JOIN {{ ref('fct_person_smoking_status') }} AS smoking ON person.person_id = smoking.person_id
LEFT JOIN {{ ref('fct_person_obesity_register') }} AS obesity ON person.person_id = obesity.person_id
LEFT JOIN {{ ref('int_cholesterol_latest') }} AS cholesterol ON person.person_id = cholesterol.person_id
