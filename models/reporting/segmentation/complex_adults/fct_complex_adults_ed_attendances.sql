{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Urgent & emergency care attendances (all ECDS settings) in the 12 months
-- ending on the segmentation reporting date for complex adults cohort members.
-- One row per attendance; pod identifies the setting (AE-T1, AE-Other, UCC,
-- WiC, SDEC).
--
-- The cohort is joined on sk_patient_id. A small number of sk_patient_ids map to
-- more than one person_id (duplicate person records for the same human), which
-- would otherwise emit the same attendance once per linked person_id and double
-- count it. The QUALIFY keeps one row per attendance, attributing it to the
-- lowest person_id deterministically.

SELECT
    c.person_id,
    e.sk_patient_id,
    e.visit_occurrence_id,
    e.pod,
    e.department_type,
    e.start_date AS arrival_date,
    e.start_time AS arrival_time,
    e.end_date AS departure_date,
    e.end_time AS departure_time,
    e.organisation_id AS provider_code,
    e.organisation_name AS provider_name,
    e.site_id,
    e.site_name,
    e.chief_complaint_code,
    e.chief_complaint_desc,
    e.primary_diagnosis_code_snomed,
    e.primary_diagnosis_desc_snomed
FROM {{ ref('int_sus_uec_encounter') }} AS e
INNER JOIN {{ ref('fct_person_complex_adults') }} AS c
    ON e.sk_patient_id = c.sk_patient_id
-- Window and key filters must match int_segmentation_acute_activity, which
-- produces the ed_attendances_12mo count that decides cohort membership.
WHERE
    e.start_date BETWEEN DATEADD('month', -12, {{ segmentation_reporting_date() }})
    AND {{ segmentation_reporting_date() }}
    AND e.sk_patient_id IS NOT NULL
    AND e.sk_patient_id != '1'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e.visit_occurrence_id
    ORDER BY c.person_id
) = 1
