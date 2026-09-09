{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-person atrial fibrillation profile for the NICE AF indicators (IND128,
IND247, IND127, IND169). One row per person on the AF register. Combines the
stroke risk score history, oral anticoagulant orders, anticoagulant exceptions
and review records, so each measure applies only its own window and rule. No
registration, living or test-patient filter; consumers join
dim_person_active_patients.
*/

WITH latest_chadsvasc AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_chadsvasc_date,
        score_value AS latest_chadsvasc_score
    FROM {{ ref('int_stroke_risk_score_all') }}
    WHERE score_type = 'CHA2DS2-VASc'
        AND score_value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

latest_chads2 AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS latest_chads2_date,
        score_value AS latest_chads2_score
    FROM {{ ref('int_stroke_risk_score_all') }}
    WHERE score_type = 'CHADS2'
        AND score_value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

score_history AS (
    SELECT
        person_id,
        MAX(score_value) AS max_stroke_risk_score_ever
    FROM {{ ref('int_stroke_risk_score_all') }}
    GROUP BY person_id
),

exceptions AS (
    SELECT
        person_id,
        BOOLOR_AGG(exception_type = 'ANTICOAGULANT_ADVERSE_REACTION') AS has_anticoagulant_adverse_reaction,
        MAX(CASE WHEN exception_type = 'ANTICOAGULANT_CONTRAINDICATED'
            THEN clinical_effective_date::DATE END) AS latest_anticoagulant_contraindicated_date,
        MAX(CASE WHEN exception_type = 'ANTICOAGULANT_DECLINED'
            THEN clinical_effective_date::DATE END) AS latest_anticoagulant_declined_date,
        BOOLOR_AGG(exception_type IN ('VALVULAR_AF', 'ANTIPHOSPHOLIPID_SYNDROME')) AS is_doac_ineligible,
        BOOLOR_AGG(exception_type IN ('DOAC_CONTRAINDICATED', 'DOAC_DECLINED', 'DOAC_NOT_INDICATED')) AS has_doac_exception
    FROM {{ ref('int_anticoagulant_exception_all') }}
    GROUP BY person_id
),

reviews AS (
    SELECT
        person_id,
        MAX(clinical_effective_date::DATE) AS latest_anticoagulant_review_date
    FROM {{ ref('int_anticoagulant_review_all') }}
    GROUP BY person_id
)

SELECT
    af.person_id,
    af.earliest_diagnosis_date::DATE AS earliest_af_diagnosis_date,
    chadsvasc.latest_chadsvasc_score,
    chadsvasc.latest_chadsvasc_date,
    chads2.latest_chads2_score,
    chads2.latest_chads2_date,
    history.max_stroke_risk_score_ever,
    therapy.latest_anticoagulant_order_date,
    therapy.latest_anticoagulant_type,
    therapy.latest_doac_order_date,
    therapy.latest_vka_order_date,
    COALESCE(exceptions.has_anticoagulant_adverse_reaction, FALSE) AS has_anticoagulant_adverse_reaction,
    exceptions.latest_anticoagulant_contraindicated_date,
    exceptions.latest_anticoagulant_declined_date,
    COALESCE(exceptions.is_doac_ineligible, FALSE) AS is_doac_ineligible,
    COALESCE(exceptions.has_doac_exception, FALSE) AS has_doac_exception,
    reviews.latest_anticoagulant_review_date
FROM {{ ref('fct_person_atrial_fibrillation_register') }} AS af
LEFT JOIN latest_chadsvasc AS chadsvasc ON af.person_id = chadsvasc.person_id
LEFT JOIN latest_chads2 AS chads2 ON af.person_id = chads2.person_id
LEFT JOIN score_history AS history ON af.person_id = history.person_id
LEFT JOIN {{ ref('int_antithrombotic_therapy_latest') }} AS therapy ON af.person_id = therapy.person_id
LEFT JOIN exceptions ON af.person_id = exceptions.person_id
LEFT JOIN reviews ON af.person_id = reviews.person_id
WHERE af.is_on_register
