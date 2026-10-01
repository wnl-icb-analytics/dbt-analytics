-- PDS vs OLIDS Practice-Level Comparison Debug
-- Shows per-practice counts from both systems side by side
-- to identify where they diverge
--
-- Usage: dbt compile -s pds_olids_practice_level_debug

WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code, prac.organisation_name
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- PDS final counts (matching int_pds_olids_practice_registration_comparison logic)
pds_counts AS (
    SELECT
        reg.practice_code,
        COUNT(DISTINCT reg.sk_patient_id) AS pds_unmerged,
        COUNT(DISTINCT COALESCE(merg.sk_patient_id_superseded, reg.sk_patient_id)) AS pds_merged
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
    LEFT JOIN {{ ref('stg_pds_person_merger') }} merg
        ON reg.sk_patient_id = merg.sk_patient_id
    LEFT JOIN {{ ref('stg_pds_person') }} per
        ON reg.sk_patient_id = per.sk_patient_id
        AND CURRENT_DATE() BETWEEN per.event_from_date
            AND COALESCE(per.event_to_date, '9999-12-31')
    LEFT JOIN {{ ref('stg_pds_reason_for_removal') }} reas
        ON reg.sk_patient_id = reas.sk_patient_id
        AND CURRENT_DATE() BETWEEN reas.event_from_date
            AND COALESCE(reas.event_to_date, '9999-12-31')
    WHERE reg.sk_patient_id IS NOT NULL
        AND CURRENT_DATE() BETWEEN reg.event_from_date
            AND COALESCE(reg.event_to_date, '9999-12-31')
        AND per.death_status IS NULL
        AND per.date_of_death IS NULL
        AND reas.reason_for_removal IS NULL
    GROUP BY reg.practice_code
),

-- OLIDS counts by sk_patient_id
olids_by_sk AS (
    SELECT
        practice_ods_code AS practice_code,
        COUNT(DISTINCT sk_patient_id) AS olids_sk_patient_count
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
    GROUP BY practice_ods_code
),

-- OLIDS counts by person_id (for comparison)
olids_by_person AS (
    SELECT
        practice_ods_code AS practice_code,
        COUNT(DISTINCT person_id) AS olids_person_count
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
    GROUP BY practice_ods_code
),

-- OLIDS: patients with sk_patient_id IS NULL
olids_null_sk AS (
    SELECT
        practice_ods_code AS practice_code,
        COUNT(*) AS null_sk_count
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
        AND sk_patient_id IS NULL
    GROUP BY practice_ods_code
)

SELECT
    COALESCE(p.practice_code, o.practice_code) AS practice_code,
    np.organisation_name,
    p.pds_merged,
    o.olids_sk_patient_count AS olids_by_sk,
    op.olids_person_count AS olids_by_person,
    COALESCE(ns.null_sk_count, 0) AS olids_null_sk,
    o.olids_sk_patient_count - p.pds_merged AS diff_sk_vs_pds,
    ROUND(100.0 * (o.olids_sk_patient_count - p.pds_merged) / NULLIF(p.pds_merged, 0), 2) AS pct_diff,
    CASE
        WHEN ABS(100.0 * (o.olids_sk_patient_count - p.pds_merged) / NULLIF(p.pds_merged, 0)) < 2
            OR ABS(o.olids_sk_patient_count - p.pds_merged) < 5
        THEN 'PASS' ELSE 'FAIL'
    END AS acceptance
FROM pds_counts p
FULL OUTER JOIN olids_by_sk o ON p.practice_code = o.practice_code
LEFT JOIN olids_by_person op ON COALESCE(p.practice_code, o.practice_code) = op.practice_code
LEFT JOIN olids_null_sk ns ON COALESCE(p.practice_code, o.practice_code) = ns.practice_code
LEFT JOIN ncl_practices np ON COALESCE(p.practice_code, o.practice_code) = np.practice_code
ORDER BY ABS(COALESCE(o.olids_sk_patient_count, 0) - COALESCE(p.pds_merged, 0)) DESC
