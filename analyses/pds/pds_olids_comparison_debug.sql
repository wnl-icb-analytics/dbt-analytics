-- PDS vs OLIDS Comparison Debug
-- Breaks down each filtering step to see where counts diverge
--
-- Usage: dbt compile -s pds_olids_comparison_debug

-- Step-by-step PDS count reduction to understand filter impact
WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code, prac.organisation_name
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- PDS Step 1: All care practice records for NCL practices
pds_all AS (
    SELECT reg.practice_code, reg.sk_patient_id
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
    WHERE reg.sk_patient_id IS NOT NULL
),

-- PDS Step 2: Currently active care practice records
pds_current AS (
    SELECT reg.practice_code, reg.sk_patient_id
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
    WHERE reg.sk_patient_id IS NOT NULL
        AND CURRENT_DATE() BETWEEN reg.event_from_date
            AND COALESCE(reg.event_to_date, '9999-12-31')
),

-- PDS Step 3: Remove deceased
pds_alive AS (
    SELECT reg.practice_code, reg.sk_patient_id
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
    LEFT JOIN {{ ref('stg_pds_person') }} per
        ON reg.sk_patient_id = per.sk_patient_id
        AND CURRENT_DATE() BETWEEN per.event_from_date
            AND COALESCE(per.event_to_date, '9999-12-31')
    WHERE reg.sk_patient_id IS NOT NULL
        AND CURRENT_DATE() BETWEEN reg.event_from_date
            AND COALESCE(reg.event_to_date, '9999-12-31')
        AND per.death_status IS NULL
        AND per.date_of_death IS NULL
),

-- PDS Step 4: Remove reason_for_removal
pds_no_removal AS (
    SELECT reg.practice_code, reg.sk_patient_id
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
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

-- PDS Step 5: Apply merger dedup (final PDS count)
pds_merged AS (
    SELECT
        reg.practice_code,
        COALESCE(merg.sk_patient_id_superseded, reg.sk_patient_id) AS merged_id
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

-- OLIDS current registrations (using int_patient_registrations)
olids_current AS (
    SELECT practice_ods_code AS practice_code, sk_patient_id
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
),

-- Aggregate counts per step
step_counts AS (
    SELECT 'PDS 1: All NCL records' AS step,
        COUNT(DISTINCT practice_code) AS practices,
        COUNT(DISTINCT sk_patient_id) AS patients
    FROM pds_all
    UNION ALL
    SELECT 'PDS 2: Currently active',
        COUNT(DISTINCT practice_code), COUNT(DISTINCT sk_patient_id)
    FROM pds_current
    UNION ALL
    SELECT 'PDS 3: Remove deceased',
        COUNT(DISTINCT practice_code), COUNT(DISTINCT sk_patient_id)
    FROM pds_alive
    UNION ALL
    SELECT 'PDS 4: Remove reason_for_removal',
        COUNT(DISTINCT practice_code), COUNT(DISTINCT sk_patient_id)
    FROM pds_no_removal
    UNION ALL
    SELECT 'PDS 5: After merger dedup',
        COUNT(DISTINCT practice_code), COUNT(DISTINCT merged_id)
    FROM pds_merged
    UNION ALL
    SELECT 'OLIDS: Current registrations',
        COUNT(DISTINCT practice_code), COUNT(DISTINCT sk_patient_id)
    FROM olids_current
)

SELECT * FROM step_counts
ORDER BY step
