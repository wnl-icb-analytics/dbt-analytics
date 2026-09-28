{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Children with complexity criteria for population segmentation.
-- Grain: one row per person aged under 18 in dim_person_demographics,
-- regardless of registration status (filter is_active = TRUE for the
-- currently registered population). All under-18s are kept, with each
-- criterion exposed as a count and flag, so the marginal contribution of
-- any single criterion can be measured; meets_any_criterion drives the
-- children with complexity segment.
--
-- Composes the segmentation building blocks; the per-source definitions
-- (windows, code lists, exclusions) live in the block models:
--   2+ qualifying LTCs (int_segmentation_ltc_count; 2+ rather than the
--       circulated 1+ so a single LTC reads as health needs (segment 2)
--       rather than complexity, keeping segment 2 reachable)
--   1+ coded complexity diagnosis (int_segmentation_children_dx,
--       the NWL CLDCHN code list, ever recorded)
--   5+ attended paediatric outpatient appointments in 12 months
--       (int_segmentation_op_activity)
--   attended outpatient care across 2+ main specialties in 12 months,
--       excluding trauma & orthopaedics, ENT, ophthalmology, A&E,
--       obstetrics and midwifery
--       (int_segmentation_op_activity, aligned to NWL)
--   1+ mental health inpatient stay in 12 months
--       (int_segmentation_mh_inpatient_activity, spells overlapping the
--       window so long-stay admissions before it still count)
--   7+ attended community service contacts in 12 months
--       excluding Health Visiting Service contacts
--       (int_segmentation_community_activity; counts are a floor)

WITH children AS (
    SELECT
        d.person_id,
        d.sk_patient_id,
        d.is_active,
        d.age,

        ZEROIFNULL(l.ltc_count) AS ltc_count,
        ZEROIFNULL(l.ltc_count) >= 2 AS has_2plus_ltcs,

        ZEROIFNULL(dx.complexity_diagnosis_codes) AS complexity_diagnosis_codes,
        dx.latest_complexity_diagnosis_date,
        dx.person_id IS NOT NULL AS has_complexity_diagnosis,

        ZEROIFNULL(op.paediatric_op_appointments_12mo)
            AS paediatric_op_appointments_12mo,
        ZEROIFNULL(op.paediatric_op_appointments_12mo) >= 5
            AS has_5plus_paediatric_op_appointments,

        ZEROIFNULL(op.outpatient_specialties_12mo)
            AS outpatient_specialties_12mo,
        ZEROIFNULL(op.outpatient_specialties_excluding_maternity_12mo)
            AS outpatient_specialties_excluding_maternity_12mo,
        ZEROIFNULL(op.outpatient_specialties_excluding_maternity_12mo) >= 2
            AS has_2plus_outpatient_specialties,

        ZEROIFNULL(mh.mh_inpatient_stays_12mo) AS mh_inpatient_stays_12mo,
        ZEROIFNULL(mh.mh_inpatient_stays_total) AS mh_inpatient_stays_total,
        ZEROIFNULL(mh.mh_inpatient_stays_12mo) >= 1 AS has_mh_inpatient_stay,

        ZEROIFNULL(cc.community_contacts_12mo) AS community_contacts_12mo,
        ZEROIFNULL(cc.community_contacts_excluding_health_visiting_12mo)
            AS community_contacts_excluding_health_visiting_12mo,
        ZEROIFNULL(cc.community_contacts_excluding_health_visiting_12mo) >= 7
            AS has_7plus_community_contacts

    FROM {{ ref('dim_person_demographics') }} AS d
    LEFT JOIN {{ ref('int_segmentation_ltc_count') }} AS l
        ON d.person_id = l.person_id
    LEFT JOIN {{ ref('int_segmentation_children_dx') }} AS dx
        ON d.person_id = dx.person_id
    LEFT JOIN {{ ref('int_segmentation_op_activity') }} AS op
        ON d.sk_patient_id = op.sk_patient_id
    LEFT JOIN {{ ref('int_segmentation_mh_inpatient_activity') }} AS mh
        ON d.sk_patient_id = mh.sk_patient_id
    LEFT JOIN {{ ref('int_segmentation_community_activity') }} AS cc
        ON d.sk_patient_id = cc.sk_patient_id
    WHERE d.age < 18
)

SELECT
    *,
    (
        has_2plus_ltcs
        OR has_complexity_diagnosis
        OR has_5plus_paediatric_op_appointments
        OR has_2plus_outpatient_specialties
        OR has_mh_inpatient_stay
        OR has_7plus_community_contacts
    ) AS meets_any_criterion,
    (
        has_2plus_ltcs::INT
        + has_complexity_diagnosis::INT
        + has_5plus_paediatric_op_appointments::INT
        + has_2plus_outpatient_specialties::INT
        + has_mh_inpatient_stay::INT
        + has_7plus_community_contacts::INT
    ) AS complexity_criteria_count
FROM children
