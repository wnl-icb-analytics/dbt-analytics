{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- GP activity block for segmentation. Grain: one row per person with at
-- least one attended clinical GP appointment in the 12 months ending on the
-- segmentation reporting date.
--
-- Attended clinical appointments only (int_appointment_gp_clinical_recent;
-- DNAs and admin excluded). Absence of a row means zero attended clinical
-- appointments in the window - fct_person_complex_adults relies on this
-- for both the >=15 appointments activity criterion and the no-GP side of
-- the high acute use criterion.

SELECT
    a.person_id,
    COUNT(*) AS gp_appointments_12mo
FROM {{ ref('int_appointment_gp_clinical_recent') }} AS a
WHERE
    a.is_attended
    AND CAST(a.start_date AS DATE)
    BETWEEN DATEADD('month', -12, {{ segmentation_reporting_date() }})
    AND {{ segmentation_reporting_date() }}
GROUP BY a.person_id
