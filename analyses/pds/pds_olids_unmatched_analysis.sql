-- PDS vs OLIDS: Unmatched Patient Analysis
-- For patients in one system but not the other, what can we learn?
--
-- Usage: dbt compile -s pds_olids_unmatched_analysis

WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- PDS currently registered (after all filters)
pds_patients AS (
    SELECT DISTINCT
        COALESCE(merg.sk_patient_id_superseded, reg.sk_patient_id) AS sk_patient_id,
        reg.practice_code
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
),

-- OLIDS currently registered
olids_patients AS (
    SELECT DISTINCT sk_patient_id, practice_ods_code AS practice_code
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
        AND sk_patient_id IS NOT NULL
),

-- OLIDS-only patients: in OLIDS but not PDS
-- Check if these patients exist in PDS at all (any record, not just current)
olids_only AS (
    SELECT o.sk_patient_id, o.practice_code
    FROM olids_patients o
    LEFT JOIN pds_patients p ON o.sk_patient_id = p.sk_patient_id
    WHERE p.sk_patient_id IS NULL
),

olids_only_pds_check AS (
    SELECT
        CASE
            WHEN cp.sk_patient_id IS NOT NULL AND CURRENT_DATE() BETWEEN cp.event_from_date
                AND COALESCE(cp.event_to_date, '9999-12-31')
                THEN 'In PDS current but filtered out'
            WHEN cp.sk_patient_id IS NOT NULL
                THEN 'In PDS but not current (historical)'
            ELSE 'Not in PDS care_practice at all'
        END AS pds_status,
        COUNT(DISTINCT oo.sk_patient_id) AS patients
    FROM olids_only oo
    LEFT JOIN {{ ref('stg_pds_patient_care_practice') }} cp
        ON oo.sk_patient_id = cp.sk_patient_id
    GROUP BY 1
),

-- For OLIDS-only patients that ARE in PDS current but filtered:
-- what filtered them out?
olids_only_filter_reason AS (
    SELECT
        CASE
            WHEN per.death_status IS NOT NULL OR per.date_of_death IS NOT NULL
                THEN 'PDS: deceased'
            WHEN reas.reason_for_removal IS NOT NULL
                THEN 'PDS: reason_for_removal=' || reas.reason_for_removal
            WHEN per.sk_patient_id IS NULL
                THEN 'PDS: no person record'
            ELSE 'PDS: unknown filter'
        END AS filter_reason,
        COUNT(DISTINCT oo.sk_patient_id) AS patients
    FROM olids_only oo
    INNER JOIN {{ ref('stg_pds_patient_care_practice') }} cp
        ON oo.sk_patient_id = cp.sk_patient_id
        AND CURRENT_DATE() BETWEEN cp.event_from_date
            AND COALESCE(cp.event_to_date, '9999-12-31')
    LEFT JOIN {{ ref('stg_pds_person') }} per
        ON oo.sk_patient_id = per.sk_patient_id
        AND CURRENT_DATE() BETWEEN per.event_from_date
            AND COALESCE(per.event_to_date, '9999-12-31')
    LEFT JOIN {{ ref('stg_pds_reason_for_removal') }} reas
        ON oo.sk_patient_id = reas.sk_patient_id
        AND CURRENT_DATE() BETWEEN reas.event_from_date
            AND COALESCE(reas.event_to_date, '9999-12-31')
    GROUP BY 1
),

-- PDS-only patients: what's their OLIDS status?
pds_only AS (
    SELECT p.sk_patient_id, p.practice_code
    FROM pds_patients p
    LEFT JOIN olids_patients o ON p.sk_patient_id = o.sk_patient_id
    WHERE o.sk_patient_id IS NULL
),

pds_only_olids_check AS (
    SELECT
        CASE
            WHEN reg.sk_patient_id IS NOT NULL AND reg.is_current_registration = TRUE
                THEN 'In OLIDS current (practice mismatch or NULL practice)'
            WHEN reg.sk_patient_id IS NOT NULL
                THEN 'In OLIDS but not current (historical)'
            WHEN pat.sk_patient_id IS NOT NULL
                THEN 'In OLIDS patient table but no registration'
            ELSE 'Not in OLIDS at all'
        END AS olids_status,
        COUNT(DISTINCT po.sk_patient_id) AS patients
    FROM pds_only po
    LEFT JOIN {{ ref('int_patient_registrations') }} reg
        ON po.sk_patient_id = reg.sk_patient_id
    LEFT JOIN {{ ref('stg_olids_patient') }} pat
        ON po.sk_patient_id = pat.sk_patient_id
    GROUP BY 1
)

SELECT 'OLIDS-only: PDS status' AS analysis, pds_status AS category, patients
FROM olids_only_pds_check
UNION ALL
SELECT 'OLIDS-only: PDS filter reason', filter_reason, patients
FROM olids_only_filter_reason
UNION ALL
SELECT 'PDS-only: OLIDS status', olids_status, patients
FROM pds_only_olids_check
ORDER BY analysis, patients DESC
