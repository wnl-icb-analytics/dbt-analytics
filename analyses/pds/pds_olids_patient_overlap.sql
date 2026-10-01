-- PDS vs OLIDS Patient-Level Overlap
-- Checks how many patients exist in PDS only, OLIDS only, or both
-- Uses sk_patient_id as the bridge between systems
--
-- Usage: dbt compile -s pds_olids_patient_overlap

WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- PDS currently registered patients (after all filters)
pds_patients AS (
    SELECT DISTINCT
        reg.practice_code,
        COALESCE(merg.sk_patient_id_superseded, reg.sk_patient_id) AS sk_patient_id
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

-- OLIDS currently registered patients
olids_patients AS (
    SELECT DISTINCT
        practice_ods_code AS practice_code,
        sk_patient_id
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
        AND sk_patient_id IS NOT NULL
),

-- Overall overlap (ignoring practice)
overall AS (
    SELECT
        COUNT(DISTINCT p.sk_patient_id) AS pds_total,
        COUNT(DISTINCT o.sk_patient_id) AS olids_total,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NOT NULL AND o.sk_patient_id IS NOT NULL
            THEN p.sk_patient_id END) AS in_both,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NOT NULL AND o.sk_patient_id IS NULL
            THEN p.sk_patient_id END) AS pds_only,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NULL AND o.sk_patient_id IS NOT NULL
            THEN o.sk_patient_id END) AS olids_only
    FROM (SELECT DISTINCT sk_patient_id FROM pds_patients) p
    FULL OUTER JOIN (SELECT DISTINCT sk_patient_id FROM olids_patients) o
        ON p.sk_patient_id = o.sk_patient_id
),

-- Practice-level overlap: same patient, same practice
practice_overlap AS (
    SELECT
        COUNT(*) AS matched_practice_patient_pairs,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NOT NULL AND o.sk_patient_id IS NOT NULL
            THEN p.sk_patient_id END) AS matched_patients,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NOT NULL AND o.sk_patient_id IS NULL
            THEN p.sk_patient_id END) AS pds_practice_only,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NULL AND o.sk_patient_id IS NOT NULL
            THEN o.sk_patient_id END) AS olids_practice_only
    FROM pds_patients p
    FULL OUTER JOIN olids_patients o
        ON p.sk_patient_id = o.sk_patient_id AND p.practice_code = o.practice_code
),

-- Patients in both systems but at DIFFERENT practices
practice_mismatch AS (
    SELECT COUNT(DISTINCT p.sk_patient_id) AS mismatched_practice_patients
    FROM pds_patients p
    INNER JOIN olids_patients o ON p.sk_patient_id = o.sk_patient_id
    WHERE p.practice_code != o.practice_code
)

SELECT 'Overall patient overlap' AS analysis,
    pds_total, olids_total, in_both, pds_only, olids_only,
    NULL AS mismatched_practice
FROM overall
UNION ALL
SELECT 'Practice+patient match',
    NULL, NULL, matched_patients, pds_practice_only, olids_practice_only,
    NULL
FROM practice_overlap
UNION ALL
SELECT 'Same patient, different practice',
    NULL, NULL, NULL, NULL, NULL,
    mismatched_practice_patients
FROM practice_mismatch
