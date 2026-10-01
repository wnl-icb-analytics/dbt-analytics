{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'],
        tags=['qadmissions', 'risk_scores'],
        meta={
            'custom_message': 'QAdmissions feature set for Snowflake model registry. Derived from the QAdmissions 2013 v4.1 reference implementation (AGPL-3.0, ClinRisk Ltd). Displaying any score produced from these features requires the ClinRisk disclaimer.'
        }
    )
}}

/*
    int_qadmissions_features
    ------------------------
    Assembles the input feature set consumed by the QAdmissions model registered
    in Snowflake's model registry. This model does not compute the risk score
    itself - it generates one row per eligible person with every feature
    column the registered model expects.

    Grain
      One row per active, non-deceased person in dim_person_demographics whose
      recorded gender is Male or Female, age is between 18-100, townsend score is 
      not null

    AGPL / ClinRisk attribution
      QAdmissions is published by ClinRisk Ltd under the GNU Affero General
      Public License v3. The ClinRisk additional terms require that the
      following disclaimer be displayed alongside any score produced from a
      derivative implementation:

      "The initial version of this file, to be found at http://qadmissions.org,
       faithfully implements QAdmissions. We have released this code under the
       GNU Affero General Public License to enable others to implement the
       algorithm faithfully. However, the nature of the GNU Affero General
       Public License is such that we cannot prevent, for example, someone
       accidentally altering the coefficients, getting the inputs wrong, or
       just poor programming. We stress, therefore, that it is the
       responsibility of the end user to check that the source that they
       receive produces the same results as the original code posted at
       http://qadmissions.org. Inaccurate implementations of risk scores can
       lead to wrong patients being given the wrong treatment."
*/

WITH base_spine AS (
    SELECT
        person_id,
        sk_patient_id,
        age,
        gender
    FROM {{ ref('dim_person_demographics') }}
    WHERE is_active = TRUE
      AND is_deceased = FALSE
      AND gender IN ('Male', 'Female')
      AND age BETWEEN 18 AND 100
),

conditions AS (
    SELECT
        person_id,
        has_atrial_fibrillation,
        has_heart_failure,
        has_cancer,
        has_asthma,
        has_copd,
        has_epilepsy,
        has_chronic_kidney_disease,
        has_severe_mental_illness,
        has_coronary_heart_disease,
        has_stroke_tia,
        has_peripheral_arterial_disease
    FROM {{ ref('dim_person_conditions') }}
),

bmi AS (
    SELECT
        person_id,
        bmi_value
    FROM {{ ref('int_bmi_latest') }}
),

diabetes AS (
    SELECT
        person_id,
        diabetes_type
    FROM {{ ref('fct_person_diabetes_register') }}
),

smoking AS (
    SELECT
        person_id,
        is_current_smoker,
        is_ex_smoker
    FROM {{ ref('int_smoking_status_latest') }}
),

-- Union of all recent (<=6m) medication orders across the five relevant classes.
-- Each row is one medication order; the med_class literal identifies the class.
medication_orders AS (
    SELECT person_id, 'anticoagulant'  AS med_class
    FROM {{ ref('int_anticoagulant_medications_all') }}
    WHERE is_recent_6m
    UNION ALL
    SELECT person_id, 'antidepressant' AS med_class
    FROM {{ ref('int_antidepressant_medications_all') }}
    WHERE is_recent_6m
    UNION ALL
    SELECT person_id, 'antipsychotic'  AS med_class
    FROM {{ ref('int_antipsychotic_medications_all') }}
    WHERE is_recent_6m
    UNION ALL
    SELECT person_id, 'corticosteroid' AS med_class
    FROM {{ ref('int_systemic_corticosteroid_medications_all') }}
    WHERE is_recent_6m
    UNION ALL
    SELECT person_id, 'nsaid'          AS med_class
    FROM {{ ref('int_nsaid_medications_all') }}
    WHERE is_recent_6m
),

-- Aggregate to person grain. QAdmissions defines each medication boolean as
-- "prescribed in the prior six months".
medication_flags AS (
    SELECT
        person_id,
        COUNT(CASE WHEN med_class = 'anticoagulant'  THEN 1 END) >= 1 AS b_anticoagulant,
        COUNT(CASE WHEN med_class = 'antidepressant' THEN 1 END) >= 1 AS b_antidepressant,
        COUNT(CASE WHEN med_class = 'antipsychotic'  THEN 1 END) >= 1 AS b_antipsychotic,
        COUNT(CASE WHEN med_class = 'corticosteroid' THEN 1 END) >= 1 AS b_corticosteroids,
        COUNT(CASE WHEN med_class = 'nsaid'          THEN 1 END) >= 1 AS b_nsaid
    FROM medication_orders
    GROUP BY person_id
),

-- Emergency inpatient spells in the prior 12 months, capped at 3 to produce
-- the QAdmissions hes_admitprior_cat bucket (0 / 1 / 2 / 3+). Sourced from
-- fct_person_sus_apc_recent.apc_nel_12mo (non-elective emergency spells,
-- admission_method codes 21-28 / 2A-2D). Keys on sk_patient_id.
emergency_admissions AS (
    SELECT
        sk_patient_id,
        LEAST(apc_nel_12mo, 3) AS hes_admitprior_cat
    FROM {{ ref('fct_person_sus_apc_recent') }}
),

-- Lab thresholds from the qadmissions_lab_thresholds seed, pivoted to one row.
lab_thresholds AS (
    SELECT
        MAX(CASE WHEN measurement = 'haemoglobin' AND direction = 'low'  THEN threshold END) AS hb_threshold,
        MAX(CASE WHEN measurement = 'platelets'   AND direction = 'high' THEN threshold END) AS platelet_threshold
    FROM {{ ref('qadmissions_lab_thresholds') }}
),

-- Latest haemoglobin value per person.
hb_latest AS (
    SELECT
        person_id,
        inferred_value AS hb_value
    FROM {{ ref('int_haemoglobin_latest') }}
),

-- Latest platelet value per person.
platelets_latest AS (
    SELECT
        person_id,
        inferred_value AS platelet_value
    FROM {{ ref('int_platelets_latest') }}
),

-- Composite LFT flag per person (TRUE if any of ALT, GGT, bilirubin above threshold).
lft_latest AS (
    SELECT
        person_id,
        high_lft
    FROM {{ ref('int_lft_latest') }}
),

-- Person-level flags from the four QAdmissions paper-faithful diagnosis /
-- observation intermediates. Each collapses observation-level rows to one
-- row per person via SELECT DISTINCT, with TRUE meaning "ever coded".
falls_flags AS (
    SELECT DISTINCT person_id, TRUE AS has_falls
    FROM {{ ref('int_falls_observations_all') }}
),

malabsorption_flags AS (
    SELECT DISTINCT person_id, TRUE AS has_malabsorption
    FROM {{ ref('int_malabsorption_diagnoses_all') }}
),

vte_flags AS (
    SELECT DISTINCT person_id, TRUE AS has_vte
    FROM {{ ref('int_vte_diagnoses_all') }}
),

liver_pancreatitis_flags AS (
    SELECT DISTINCT person_id, TRUE AS has_liver_pancreatitis
    FROM {{ ref('int_liver_pancreatitis_diagnoses_all') }}
),

-- Townsend Deprivation Score per person (NULL where the LSOA does not
-- bridge to a Townsend-mapped 2011 LSOA, e.g. non-English residence).
townsend AS (
    SELECT
        person_id,
        townsend_score
    FROM {{ ref('int_qadmissions_townsend') }}
),

-- Alcohol category 0..5 derived from Full AUDIT scores. 

alcohol AS (
    SELECT
        person_id,
        alcohol_cat6
    FROM {{ ref('int_qadmissions_alcohol_category') }}
),

-- Ethrisk 1..9 derived from int_ethnicity_qof joined to the
-- qadmissions_eth2016_to_ethrisk9 seed. Persons with no ethnicity
-- record default to 1 (NotRecorded -> 1, same group as White).
ethrisk_lookup AS (
    SELECT
        person_id,
        ethrisk
    FROM {{ ref('int_qadmissions_ethrisk') }}
)

SELECT
    -- Identifiers and demographics.
    -- person_id is the OLIDS-native unique key; sk_patient_id is the
    -- commissioning-side key and the join column expected by the Snowflake
    -- model registry. Upstream dim_person_demographics can produce the same
    -- sk_patient_id for multiple person_ids (identity reconciliation edge
    -- cases) - hence the uniqueness test is on person_id.
    base.person_id,
    base.sk_patient_id,
    base.age,
    CASE WHEN base.gender = 'Male' THEN 'male' ELSE 'female' END           AS sex,
    bmi.bmi_value                                                          AS bmi,

    -- Constants
    5                                                                      AS sha1,
    {{ var('qadmissions_horizon_years', 1) }}                              AS surv,

    -- Disease booleans from dim_person_conditions
    COALESCE(cond.has_atrial_fibrillation, FALSE)                          AS b_af,
    COALESCE(cond.has_heart_failure, FALSE)                                AS b_ccf,
    COALESCE(cond.has_cancer, FALSE)                                       AS b_anycancer,
    COALESCE(cond.has_asthma, FALSE)
        OR COALESCE(cond.has_copd, FALSE)                                  AS b_asthmacopd,
    COALESCE(cond.has_epilepsy, FALSE)                                     AS b_epilepsy,
    COALESCE(cond.has_chronic_kidney_disease, FALSE)                       AS b_renal,
    COALESCE(cond.has_severe_mental_illness, FALSE)                        AS b_manicschiz,
    COALESCE(cond.has_coronary_heart_disease, FALSE)
        OR COALESCE(cond.has_stroke_tia, FALSE)
        OR COALESCE(cond.has_peripheral_arterial_disease, FALSE)           AS b_cvd,

    -- Diabetes type split. 'Unknown' maps to FALSE for both.
    COALESCE(diab.diabetes_type = 'Type 1', FALSE)                         AS b_type1,
    COALESCE(diab.diabetes_type = 'Type 2', FALSE)                         AS b_type2,

    -- Smoking category. No CPD data available so current smokers map to
    -- category 3 (light smoker); documented in plan.md decision 1.
    CASE
        WHEN smk.is_current_smoker THEN 3
        WHEN smk.is_ex_smoker      THEN 1
        ELSE                            0
    END                                                                    AS smoke_cat,

    -- Medication booleans 
    COALESCE(med.b_anticoagulant,   FALSE)                                 AS b_anticoagulant,
    COALESCE(med.b_antidepressant,  FALSE)                                 AS b_antidepressant,
    COALESCE(med.b_antipsychotic,   FALSE)                                 AS b_antipsychotic,
    COALESCE(med.b_corticosteroids, FALSE)                                 AS b_corticosteroids,
    COALESCE(med.b_nsaid,           FALSE)                                 AS b_nsaid,

    -- Prior emergency admissions (0 / 1 / 2 / 3+) in previous 365 days.
    COALESCE(adm.hes_admitprior_cat, 0)                                    AS hes_admitprior_cat,

    -- Lab features from observation intermediates.
    -- Thresholds sourced from qadmissions_lab_thresholds seed.
    COALESCE(hb.hb_value < t.hb_threshold, FALSE)                          AS c_hb,
    COALESCE(lft.high_lft, FALSE)                                          AS high_lft,
    COALESCE(plt.platelet_value > t.platelet_threshold, FALSE)             AS high_platelet,

    -- Diagnosis / observation flags from QAdmissions paper-faithful clusters.
    COALESCE(fls.has_falls, FALSE)                                         AS b_falls,
    COALESCE(mal.has_malabsorption, FALSE)                                 AS b_malabsorption,
    COALESCE(vte.has_vte, FALSE)                                           AS b_vte,
    COALESCE(lp.has_liver_pancreatitis, FALSE)                             AS b_liverpancreas,

    twn.townsend_score                                                     AS town,

    COALESCE(alc.alcohol_cat6, 0)                                          AS alcohol_cat6,

    -- Ethnicity risk group 1..9. Persons with no ethnicity record
    -- default to 1 (NotRecorded -> 1 per QResearch).
    COALESCE(eth.ethrisk, 1)                                               AS ethrisk

FROM base_spine                       base
CROSS JOIN lab_thresholds             t
LEFT JOIN conditions                  cond ON base.person_id     = cond.person_id
LEFT JOIN bmi                              ON base.person_id     = bmi.person_id
LEFT JOIN diabetes                    diab ON base.person_id     = diab.person_id
LEFT JOIN smoking                     smk  ON base.person_id     = smk.person_id
LEFT JOIN medication_flags            med  ON base.person_id     = med.person_id
LEFT JOIN emergency_admissions        adm  ON base.sk_patient_id = adm.sk_patient_id
LEFT JOIN hb_latest                   hb   ON base.person_id     = hb.person_id
LEFT JOIN platelets_latest            plt  ON base.person_id     = plt.person_id
LEFT JOIN lft_latest                  lft  ON base.person_id     = lft.person_id
LEFT JOIN falls_flags                 fls  ON base.person_id     = fls.person_id
LEFT JOIN malabsorption_flags         mal  ON base.person_id     = mal.person_id
LEFT JOIN vte_flags                   vte  ON base.person_id     = vte.person_id
LEFT JOIN liver_pancreatitis_flags    lp   ON base.person_id     = lp.person_id
LEFT JOIN townsend                    twn  ON base.person_id     = twn.person_id
LEFT JOIN alcohol                     alc  ON base.person_id     = alc.person_id
LEFT JOIN ethrisk_lookup              eth  ON base.person_id     = eth.person_id