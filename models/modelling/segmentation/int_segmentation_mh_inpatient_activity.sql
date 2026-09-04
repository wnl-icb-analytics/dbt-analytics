{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'])
}}

-- Mental health inpatient activity block for segmentation. Grain: one row
-- per sk_patient_id with any MHSDS hospital provider spell; sk_patient_id
-- '1' is a shared junk key and is excluded.
--
-- mh_inpatient_stays_12mo counts spells overlapping the 12-month window -
-- admitted during it, discharged during it, or spanning it - so long-stay
-- patients admitted before the window still count. end_date is reliable
-- since the orphaned-spell rework: NULL only for genuinely open spells,
-- orphaned records are closed at their last submission. The window ends on
-- the latest spell start date (lag-aware); the lifetime count is also
-- exposed.

WITH mh_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_mhsds_spell_encounters') }}
    WHERE start_date <= CURRENT_DATE()
)

SELECT
    s.sk_patient_id,
    COUNT(DISTINCT s.encounter_id) AS mh_inpatient_stays_total,
    COUNT(DISTINCT CASE
        WHEN s.end_date IS NULL
            OR s.end_date >= DATEADD(MONTH, -12, m.max_date)
            THEN s.encounter_id
    END) AS mh_inpatient_stays_12mo
FROM {{ ref('int_mhsds_spell_encounters') }} AS s
CROSS JOIN mh_max_date AS m
WHERE
    s.sk_patient_id IS NOT NULL
    AND s.sk_patient_id != '1'
    AND s.start_date <= m.max_date
    AND s.start_date <= CURRENT_DATE()
GROUP BY s.sk_patient_id
