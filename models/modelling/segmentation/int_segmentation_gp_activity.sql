{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- GP activity block for segmentation. Grain: one row per person with at
-- least one attended clinical GP appointment in the rolling 12 months
-- ending on the latest attended appointment date (lag-aware).
--
-- Attended clinical appointments only (int_appointment_gp_clinical_recent;
-- DNAs and admin excluded). Absence of a row means zero attended clinical
-- appointments in the window - fct_person_complex_adults relies on this
-- for both the >=15 appointments activity criterion and the no-GP side of
-- the high acute use criterion.

WITH gp_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_appointment_gp_clinical_recent') }}
    WHERE is_attended AND start_date <= CURRENT_DATE()
)

SELECT
    a.person_id,
    COUNT(*) AS gp_appointments_12mo
FROM {{ ref('int_appointment_gp_clinical_recent') }} AS a
CROSS JOIN gp_max_date AS m
WHERE
    a.is_attended
    AND a.start_date >= DATEADD(MONTH, -12, m.max_date)
    AND a.start_date <= m.max_date
GROUP BY a.person_id
