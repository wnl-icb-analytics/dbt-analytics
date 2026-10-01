-- PDS vs OLIDS: Check unmatched patients across ALL PDS tables
-- For OLIDS-only patients, do they exist in PDS person table
-- even if they're not in care_practice?
--
-- Usage: dbt compile -s pds_olids_unmatched_pds_tables

WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- PDS currently registered (final filtered set)
pds_final AS (
    SELECT DISTINCT
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

-- OLIDS currently registered
olids_current AS (
    SELECT DISTINCT sk_patient_id
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
        AND sk_patient_id IS NOT NULL
),

-- OLIDS patients not in PDS final set
olids_only AS (
    SELECT o.sk_patient_id
    FROM olids_current o
    LEFT JOIN pds_final p ON o.sk_patient_id = p.sk_patient_id
    WHERE p.sk_patient_id IS NULL
)

-- Check each PDS table for these OLIDS-only patients
SELECT
    CASE
        WHEN cp.sk_patient_id IS NOT NULL AND per.sk_patient_id IS NOT NULL
            THEN 'In PDS person + care_practice'
        WHEN cp.sk_patient_id IS NOT NULL AND per.sk_patient_id IS NULL
            THEN 'In PDS care_practice only (no person record)'
        WHEN cp.sk_patient_id IS NULL AND per.sk_patient_id IS NOT NULL
            THEN 'In PDS person only (no care_practice)'
        ELSE 'Not in any PDS table'
    END AS pds_presence,
    COUNT(DISTINCT oo.sk_patient_id) AS patients
FROM olids_only oo
LEFT JOIN (SELECT DISTINCT sk_patient_id FROM {{ ref('stg_pds_patient_care_practice') }}) cp
    ON oo.sk_patient_id = cp.sk_patient_id
LEFT JOIN (SELECT DISTINCT sk_patient_id FROM {{ ref('stg_pds_person') }}) per
    ON oo.sk_patient_id = per.sk_patient_id
GROUP BY 1
ORDER BY patients DESC
