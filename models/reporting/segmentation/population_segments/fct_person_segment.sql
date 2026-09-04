{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Population segmentation for the ICB Strategy.
-- Grain: one row per person in dim_person_demographics — everyone with
-- registration history, including people who have left or died. Filter
-- is_active = TRUE for the currently registered population.
--
-- Every person is assigned to exactly one segment by hierarchy (highest
-- wins):
--   8  End of Life                 on the palliative care register, any age
--   7  Adults with Complexity      >= 18, complex adults cohort
--                                  (fct_person_complex_adults)
--   6  Adults with Multiple LTCs   >= 18, 2+ qualifying LTCs
--   5  Adults with a Single LTC    >= 18, 1 qualifying LTC
--   4  Mostly Healthy Adults       >= 18, none of the above
--   3  Children with Complexity    < 18, any children complexity criterion
--                                  (int_segmentation_children_complexity)
--   2  Children with Health Needs  < 18, 1+ qualifying LTC
--   1  Mostly Healthy Children     < 18, none of the above
--
-- Qualifying LTCs for segments 6/5/2 are the 16 complex adults conditions
-- plus NAFLD (int_segmentation_ltc_count). The complex adults cohort keeps
-- its own list without NAFLD.
--
-- The children with complexity LTC criterion is 2+ LTCs, so a single LTC
-- reads as health needs (segment 2) rather than complexity. See
-- int_segmentation_children_complexity for the rationale.
--
-- The underlying cohorts are not mutually exclusive (a complex adult may
-- also be on the palliative care register); each is exposed as its own
-- boolean so overlap between segments can be measured.

WITH segments AS (
    SELECT
        d.person_id,
        d.sk_patient_id,
        d.is_active,
        d.is_deceased,
        d.age,
        d.gender,
        d.practice_code,
        d.practice_name,
        d.borough_registered,
        d.neighbourhood_registered,

        -- Non-mutually-exclusive cohort flags
        COALESCE(pc.is_on_register, FALSE) AS is_end_of_life,
        ca.person_id IS NOT NULL AS is_complex_adult,
        COALESCE(d.age >= 18 AND l.ltc_count >= 2, FALSE)
            AS has_multiple_ltcs,
        COALESCE(d.age >= 18 AND l.ltc_count = 1, FALSE) AS has_single_ltc,
        COALESCE(cc.meets_any_criterion, FALSE) AS is_complex_child,
        COALESCE(d.age < 18 AND l.ltc_count >= 1, FALSE)
            AS has_child_health_needs,

        -- Drivers
        ZEROIFNULL(l.ltc_count) AS ltc_count,
        l.ltc_list,
        ca.complexity_criteria_count AS adult_complexity_criteria_count,
        cc.complexity_criteria_count AS child_complexity_criteria_count,
        cc.has_2plus_ltcs AS child_has_2plus_ltcs,
        cc.has_complexity_diagnosis AS child_has_complexity_diagnosis,
        cc.has_5plus_paediatric_op_appointments
            AS child_has_5plus_paediatric_op_appointments,
        cc.has_2plus_outpatient_specialties
            AS child_has_2plus_outpatient_specialties,
        cc.has_mh_inpatient_stay AS child_has_mh_inpatient_stay,
        cc.has_7plus_community_contacts AS child_has_7plus_community_contacts

    FROM {{ ref('dim_person_demographics') }} AS d
    LEFT JOIN {{ ref('fct_person_palliative_care_register') }} AS pc
        ON d.person_id = pc.person_id
    LEFT JOIN {{ ref('fct_person_complex_adults') }} AS ca
        ON d.person_id = ca.person_id
    LEFT JOIN {{ ref('int_segmentation_ltc_count') }} AS l
        ON d.person_id = l.person_id
    LEFT JOIN {{ ref('int_segmentation_children_complexity') }} AS cc
        ON d.person_id = cc.person_id
),

assigned AS (
    SELECT
        *,
        CASE
            WHEN is_end_of_life THEN 8
            WHEN age >= 18 AND is_complex_adult THEN 7
            WHEN has_multiple_ltcs THEN 6
            WHEN has_single_ltc THEN 5
            WHEN age >= 18 THEN 4
            WHEN is_complex_child THEN 3
            WHEN has_child_health_needs THEN 2
            ELSE 1
        END AS segment_number
    FROM segments
),

-- segment_name is derived from segment_number rather than re-evaluating the
-- hierarchy a second time, so the number and its label cannot drift apart
-- if the conditions are ever edited.
named AS (
    SELECT
        *,
        CASE segment_number
            WHEN 8 THEN 'End of Life'
            WHEN 7 THEN 'Adults with Complexity'
            WHEN 6 THEN 'Adults with Multiple LTCs'
            WHEN 5 THEN 'Adults with a Single LTC'
            WHEN 4 THEN 'Mostly Healthy Adults'
            WHEN 3 THEN 'Children with Complexity'
            WHEN 2 THEN 'Children with Health Needs'
            WHEN 1 THEN 'Mostly Healthy Children'
        END AS segment_name
    FROM assigned
)

-- Columns listed explicitly so the segment assignment leads the table
-- rather than trailing the cohort flags and drivers.
SELECT
    person_id,
    sk_patient_id,
    segment_number,
    segment_name,

    is_active,
    is_deceased,
    age,
    gender,
    practice_code,
    practice_name,
    borough_registered,
    neighbourhood_registered,

    is_end_of_life,
    is_complex_adult,
    has_multiple_ltcs,
    has_single_ltc,
    is_complex_child,
    has_child_health_needs,

    ltc_count,
    ltc_list,
    adult_complexity_criteria_count,
    child_complexity_criteria_count,
    child_has_2plus_ltcs,
    child_has_complexity_diagnosis,
    child_has_5plus_paediatric_op_appointments,
    child_has_2plus_outpatient_specialties,
    child_has_mh_inpatient_stay,
    child_has_7plus_community_contacts
FROM named
