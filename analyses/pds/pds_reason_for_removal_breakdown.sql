-- PDS Reason for Removal Breakdown
-- Explores what reason_for_removal values exist and how they affect counts
--
-- Usage: dbt compile -s pds_reason_for_removal_breakdown

WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- Current PDS registrations for NCL
current_regs AS (
    SELECT reg.sk_patient_id, reg.practice_code
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
    WHERE reg.sk_patient_id IS NOT NULL
        AND CURRENT_DATE() BETWEEN reg.event_from_date
            AND COALESCE(reg.event_to_date, '9999-12-31')
),

-- Current removal reasons
current_removals AS (
    SELECT
        reas.sk_patient_id,
        reas.reason_for_removal
    FROM {{ ref('stg_pds_reason_for_removal') }} reas
    WHERE CURRENT_DATE() BETWEEN reas.event_from_date
        AND COALESCE(reas.event_to_date, '9999-12-31')
)

-- How many NCL patients have each removal reason?
SELECT
    COALESCE(r.reason_for_removal, '(no removal record)') AS reason,
    COUNT(DISTINCT cr.sk_patient_id) AS patients,
    ROUND(100.0 * COUNT(DISTINCT cr.sk_patient_id) /
        SUM(COUNT(DISTINCT cr.sk_patient_id)) OVER (), 2) AS pct
FROM current_regs cr
LEFT JOIN current_removals r ON cr.sk_patient_id = r.sk_patient_id
GROUP BY COALESCE(r.reason_for_removal, '(no removal record)')
ORDER BY patients DESC
