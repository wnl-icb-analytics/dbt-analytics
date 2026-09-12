{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'])
}}

-- Mental health inpatient activity block for segmentation. Grain: one row
-- per sk_patient_id with any MHSDS hospital provider spell starting on or
-- before the segmentation reporting date; sk_patient_id '1' is a shared junk
-- key and is excluded.
--
-- mh_inpatient_stays_12mo counts spells overlapping the 12 months ending on
-- the reporting date - admitted during it, discharged during it, or spanning
-- it - so long-stay patients admitted before the window still count.
-- end_date is reliable since the orphaned-spell rework: NULL only for
-- genuinely open spells, orphaned records are closed at their last
-- submission. The lifetime count is also exposed.

SELECT
    s.sk_patient_id,
    COUNT(DISTINCT s.encounter_id) AS mh_inpatient_stays_total,
    COUNT(DISTINCT CASE
        WHEN s.end_date IS NULL
            OR s.end_date >= DATEADD('month', -12, {{ segmentation_reporting_date() }})
            THEN s.encounter_id
    END) AS mh_inpatient_stays_12mo
FROM {{ ref('int_mhsds_spell_encounters') }} AS s
WHERE
    s.sk_patient_id IS NOT NULL
    AND s.sk_patient_id != '1'
    AND s.start_date <= {{ segmentation_reporting_date() }}
GROUP BY s.sk_patient_id
