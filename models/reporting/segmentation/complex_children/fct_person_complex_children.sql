{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Children with complexity cohort.
-- One row per person aged under 18 meeting at least one complexity
-- criterion (int_segmentation_children_complexity):
--   2+ qualifying LTCs
--   1+ coded complexity diagnosis (NWL CLDCHN code list)
--   5+ attended paediatric outpatient appointments in 12 months
--   attended outpatient care across 2+ main specialties in 12 months
--       (excluding trauma & orthopaedics, ENT, ophthalmology, A&E,
--       obstetrics and midwifery)
--   1+ mental health inpatient stay in 12 months (MHSDS)
--   7+ attended community service contacts in 12 months, excluding Health
--       Visiting Service contacts (CSDS)
--
-- Reporting companion to fct_person_complex_adults: the same cohort that
-- fct_person_segment assigns to segment 3, surfaced with demographics and
-- per-criterion columns so the marginal contribution of any single
-- criterion can be measured. Persons on the palliative care register
-- appear here too but sit in segment 8 (End of Life) in fct_person_segment.
--
-- Includes all persons regardless of registration status; filter
-- is_active = TRUE for the currently registered population.

SELECT
    cc.person_id,
    cc.sk_patient_id,
    cc.is_active,
    cc.age,
    d.gender,
    d.practice_code,
    d.practice_name,
    d.borough_registered,
    d.neighbourhood_registered,

    cc.ltc_count,
    l.ltc_list,
    cc.has_2plus_ltcs,

    cc.complexity_diagnosis_codes,
    cc.latest_complexity_diagnosis_date,
    cc.has_complexity_diagnosis,

    cc.paediatric_op_appointments_12mo,
    cc.has_5plus_paediatric_op_appointments,

    cc.outpatient_specialties_12mo,
    cc.outpatient_specialties_excluding_maternity_12mo,
    cc.has_2plus_outpatient_specialties,

    cc.mh_inpatient_stays_12mo,
    cc.mh_inpatient_stays_total,
    cc.has_mh_inpatient_stay,

    cc.community_contacts_12mo,
    cc.community_contacts_excluding_health_visiting_12mo,
    cc.has_7plus_community_contacts,

    cc.complexity_criteria_count

FROM {{ ref('int_segmentation_children_complexity') }} AS cc
INNER JOIN {{ ref('dim_person_demographics') }} AS d
    ON cc.person_id = d.person_id
LEFT JOIN {{ ref('int_segmentation_ltc_count') }} AS l
    ON cc.person_id = l.person_id
WHERE cc.meets_any_criterion
