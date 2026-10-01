{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Non-elective admissions in the 12 months ending on the segmentation
-- reporting date for complex adults cohort members. One row per spell
-- (admission method 2x excluding 2C, baby born at home; end dates imputed for
-- open/incomplete spells).
--
-- The cohort is joined on sk_patient_id. A small number of sk_patient_ids map to
-- more than one person_id (duplicate person records for the same human), which
-- would otherwise emit the same spell once per linked person_id and double count
-- it. The QUALIFY keeps one row per spell, attributing it to the lowest person_id
-- deterministically, matching fct_complex_adults_ed_attendances.

SELECT
    c.person_id,
    s.sk_patient_id,
    s.visit_occurrence_id,
    s.start_date AS admission_date,
    s.start_time AS admission_time,
    s.end_date AS discharge_date,
    s.end_time AS discharge_time,
    s.duration AS length_of_stay_days,
    s.spell_admission_method AS admission_method,
    s.organisation_id AS provider_code,
    s.organisation_name AS provider_name
FROM {{ ref('int_sus_apc_imputed_spells') }} AS s
INNER JOIN {{ ref('fct_person_complex_adults') }} AS c
    ON s.sk_patient_id = c.sk_patient_id
-- Window and key filters must match int_segmentation_acute_activity, which
-- produces the nel_admissions_12mo count that decides cohort membership.
WHERE
    LEFT(s.spell_admission_method, 1) = '2'
    AND s.spell_admission_method != '2C'
    AND s.start_date BETWEEN DATEADD('month', -12, {{ segmentation_reporting_date() }})
    AND {{ segmentation_reporting_date() }}
    AND s.sk_patient_id IS NOT NULL
    AND s.sk_patient_id != '1'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.visit_occurrence_id
    ORDER BY c.person_id
) = 1
