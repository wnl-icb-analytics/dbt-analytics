{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Complex Adults Cohort v2.2
-- One row per person meeting all cohort criteria:
--   Age >= 18
--   AND at least one complexity criterion:
--       moderate/severe frailty (eFI2 OR coded frailty diagnosis)
--       OR 3+ included LTCs
--       OR homeless
--       OR on the palliative care register
--       OR alcohol misuse
--       OR substance misuse
--       OR high acute use with no GP contact (>=3 ED attendances OR >=2 NEL
--          admissions, AND 0 attended GP appointments, all in 12 months)
--   AND at least one activity criterion:
--       >=2 NEL admissions OR >=3 ED attendances
--       OR >=15 attended GP appointments (all rolling 12 months)
--       OR attended outpatient care across >=5 treatment-function specialties
--       OR housebound
--
-- Changes from v2.1:
--   - High acute use with no GP contact joins the complexity limb. Anyone
--     meeting it also meets the ED/NEL activity criteria, so it acts as a
--     direct entry route for people cycling through acute services with no
--     primary care footprint (and therefore no coded LTC/frailty/register
--     data to catch them via the other complexity criteria).
--
-- Changes from v2.0:
--   - GP activity joins the utilisation limb: >=15 attended clinical
--     appointments in 12 months (int_appointment_gp_clinical_recent; DNAs and
--     admin excluded). 15+ is around the active-adult 95th percentile, so
--     the criterion reads as sustained high GP use.
--   - Outpatient specialty breadth joins the utilisation limb: attended care
--     across >=5 treatment-function specialties in 12 months. Five specialties
--     is around the active-adult 95th percentile.
--
-- Changes from v1.0 (criteria signed off by K Saravanakumar, 27 Jul 2026):
--   - Complexity limb widened from 2 alternatives to 6.
--   - Frailty is now eFI2 OR coded diagnosis, rather than coded diagnosis alone.
--   - Learning disability joins the LTC list, counted only at age >= 65.
--
-- Includes all persons regardless of registration status; filter is_active = TRUE
-- for the currently registered population.
--
-- Included LTCs (16): AF, Asthma, CHD, CKD, COPD, Dementia, Depression, Diabetes,
-- Epilepsy, Heart Failure, Hypertension, SMI, Stroke/TIA, Parkinson's, Anxiety,
-- and Learning Disability (65+ only).
--
-- Frailty is deliberately additive. eFI2 is only scored at age >= 65 (the index
-- was developed and validated in that population) and int_efi2_patient_list also
-- excludes deceased persons, so eFI2 alone would blank the frailty limb for every
-- under-65 and every deceased row this table intentionally retains. The coded
-- frailty register has neither restriction and covers that gap. This mirrors
-- cltcs_adult_population, which combines the same two sources. Coded severity
-- uses latest-record-wins rather than ever-coded.
--
-- ED attendances count all urgent & emergency care settings from ECDS
-- (Type 1/2 A&E, UTC, WiC, SDEC).
-- GP and outpatient 12-month windows end on the latest available activity date
-- in each source to account for reporting lag.
--
-- Every complexity criterion is exposed as its own column alongside
-- complexity_criteria_count, so the marginal contribution of any single
-- criterion can be measured without rebuilding the model.

WITH complexity AS (
    SELECT
        d.person_id,
        d.sk_patient_id,
        d.is_active,
        d.age,
        d.gender,
        d.practice_code,
        d.practice_name,
        d.borough_registered,
        d.neighbourhood_registered,

        -- Condition flags
        COALESCE(l.has_af, FALSE) AS has_af,
        COALESCE(l.has_asthma, FALSE) AS has_asthma,
        COALESCE(l.has_chd, FALSE) AS has_chd,
        COALESCE(l.has_ckd, FALSE) AS has_ckd,
        COALESCE(l.has_copd, FALSE) AS has_copd,
        COALESCE(l.has_dementia, FALSE) AS has_dementia,
        COALESCE(l.has_depression, FALSE) AS has_depression,
        COALESCE(l.has_diabetes, FALSE) AS has_diabetes,
        COALESCE(l.has_epilepsy, FALSE) AS has_epilepsy,
        COALESCE(l.has_heart_failure, FALSE) AS has_heart_failure,
        COALESCE(l.has_hypertension, FALSE) AS has_hypertension,
        COALESCE(l.has_smi, FALSE) AS has_smi,
        COALESCE(l.has_stroke_tia, FALSE) AS has_stroke_tia,
        COALESCE(l.has_parkinsons, FALSE) AS has_parkinsons,
        COALESCE(l.has_anxiety, FALSE) AS has_anxiety,
        COALESCE(l.has_learning_disability, FALSE) AS has_learning_disability,
        COALESCE(l.ltc_count, 0) AS ltc_count,
        COALESCE(l.ltc_count >= 3, FALSE) AS has_3plus_ltcs,

        -- Frailty: eFI2 (65+, living only) and coded diagnosis, combined
        e.efi_score AS efi2_score,
        e.category AS efi2_category,
        COALESCE(
            e.category IN ('MODERATE FRAILTY', 'SEVERE FRAILTY'), FALSE
        ) AS has_efi2_moderate_severe_frailty,
        f.latest_frailty_severity,
        COALESCE(
            f.latest_frailty_severity IN ('Moderate', 'Severe'), FALSE
        ) AS has_coded_moderate_severe_frailty,
        COALESCE(
            e.category IN ('MODERATE FRAILTY', 'SEVERE FRAILTY')
            OR f.latest_frailty_severity IN ('Moderate', 'Severe'),
            FALSE
        ) AS has_moderate_severe_frailty,

        -- Homeless: presence in dim_person_homeless means the latest residential
        -- code is a homelessness code, or the person is registered with the
        -- Camden Health Improvement Practice
        hom.person_id IS NOT NULL AS is_homeless,

        -- Palliative care register (QOF)
        COALESCE(pc.is_on_register, FALSE) AS is_on_palliative_care_register,

        -- Alcohol misuse: any ALCOHOL_MISUSE_DISORDERS record, ever. The cluster
        -- is diagnosis-led and includes alcohol-caused disease (alcoholic liver
        -- disease, alcohol-related pancreatitis, alcoholic cardiomyopathy), which
        -- does not resolve, so ever-coded is the right read rather than
        -- has_active_disorder, which depends on the code being flagged a problem.
        COALESCE(alc.disorder_record_count > 0, FALSE) AS has_alcohol_misuse,
        alc.latest_disorder_date AS latest_alcohol_disorder_date,

        -- Substance misuse: latest ILLSUB_COD record is QUALIFYING
        COALESCE(sub.has_substance_misuse, FALSE) AS has_substance_misuse,
        sub.latest_qualifying_date AS latest_substance_misuse_date,

        -- High acute use with no GP contact: (>=3 ED attendances OR >=2 NEL
        -- admissions in 12 months) AND no attended GP appointment in the GP
        -- 12-month window. No row in int_segmentation_gp_activity means zero
        -- attended clinical appointments, so the GP side matches the >=15
        -- criterion's definition (DNAs and admin excluded, lag-aware window).
        (
            (ZEROIFNULL(ae.ae_tot_12mo) >= 3 OR ZEROIFNULL(ip.apc_nel_12mo) >= 2)
            AND gp.person_id IS NULL
        ) AS has_high_acute_use_no_gp,

        -- Utilisation
        ZEROIFNULL(ip.apc_nel_12mo) AS nel_admissions_12mo,
        ZEROIFNULL(ae.ae_tot_12mo) AS ed_attendances_12mo,
        ZEROIFNULL(gp.gp_appointments_12mo) AS gp_appointments_12mo,
        ZEROIFNULL(op.op_spec_12mo) AS outpatient_specialties_12mo,
        COALESCE(h.is_housebound, FALSE) AS is_housebound

    FROM {{ ref('dim_person_demographics') }} AS d
    LEFT JOIN {{ ref('int_segmentation_complex_adults_ltc') }} AS l
        ON d.person_id = l.person_id
    LEFT JOIN {{ ref('fct_person_efi2') }} AS e
        ON d.person_id = e.person_id
    LEFT JOIN {{ ref('fct_person_frailty_register') }} AS f
        ON d.person_id = f.person_id
    LEFT JOIN {{ ref('dim_person_homeless') }} AS hom
        ON d.person_id = hom.person_id
    LEFT JOIN {{ ref('fct_person_palliative_care_register') }} AS pc
        ON d.person_id = pc.person_id
    LEFT JOIN {{ ref('fct_person_alcohol_status') }} AS alc
        ON d.person_id = alc.person_id
    LEFT JOIN {{ ref('int_substance_misuse_status') }} AS sub
        ON d.person_id = sub.person_id
    LEFT JOIN {{ ref('int_segmentation_gp_activity') }} AS gp
        ON d.person_id = gp.person_id
    LEFT JOIN {{ ref('fct_person_sus_op_recent') }} AS op
        ON d.sk_patient_id = op.sk_patient_id
    LEFT JOIN {{ ref('dim_person_housebound_status') }} AS h
        ON d.person_id = h.person_id
    LEFT JOIN {{ ref('fct_person_sus_apc_recent') }} AS ip
        ON d.sk_patient_id = ip.sk_patient_id
    LEFT JOIN {{ ref('fct_person_sus_uec_recent') }} AS ae
        ON d.sk_patient_id = ae.sk_patient_id
    WHERE d.age >= 18
)

SELECT
    *,
    -- How many of the seven complexity criteria the person meets. A value of 1
    -- means removing that single criterion would remove the person from the
    -- cohort, so this supports marginal-contribution reporting directly.
    (
        has_moderate_severe_frailty::INT
        + has_3plus_ltcs::INT
        + is_homeless::INT
        + is_on_palliative_care_register::INT
        + has_alcohol_misuse::INT
        + has_substance_misuse::INT
        + has_high_acute_use_no_gp::INT
    ) AS complexity_criteria_count

FROM complexity
-- Clinical complexity
WHERE (
    has_moderate_severe_frailty
    OR has_3plus_ltcs
    OR is_homeless
    OR is_on_palliative_care_register
    OR has_alcohol_misuse
    OR has_substance_misuse
    OR has_high_acute_use_no_gp
)
-- Activity or housebound
AND (
    nel_admissions_12mo >= 2
    OR ed_attendances_12mo >= 3
    OR gp_appointments_12mo >= 15
    OR outpatient_specialties_12mo >= 5
    OR is_housebound
)
