-- PDS vs OLIDS: sk_patient_id Coverage
-- Checks how well sk_patient_id bridges the two systems
-- and whether NULL sk_patient_ids explain the OLIDS-only patients
--
-- Usage: dbt compile -s pds_olids_sk_patient_id_coverage

-- OLIDS: how many current registrations have NULL sk_patient_id?
WITH olids_sk_coverage AS (
    SELECT
        COUNT(*) AS total_current,
        COUNT(sk_patient_id) AS with_sk,
        COUNT(*) - COUNT(sk_patient_id) AS without_sk,
        COUNT(DISTINCT person_id) AS distinct_persons,
        COUNT(DISTINCT sk_patient_id) AS distinct_sk
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
        AND practice_ods_code IS NOT NULL
),

-- OLIDS: what does the patient table look like for sk_patient_id coverage?
olids_patient_sk AS (
    SELECT
        COUNT(*) AS total_patients,
        COUNT(sk_patient_id) AS with_sk,
        COUNT(*) - COUNT(sk_patient_id) AS without_sk,
        COUNT(DISTINCT sk_patient_id) AS distinct_sk
    FROM {{ ref('stg_olids_patient') }}
),

-- How many distinct sk_patient_ids in each system?
pds_sk AS (
    SELECT COUNT(DISTINCT sk_patient_id) AS pds_distinct_sk
    FROM {{ ref('stg_pds_patient_care_practice') }}
    WHERE sk_patient_id IS NOT NULL
),

-- Overlap of sk_patient_ids between systems (all, not just current)
sk_overlap AS (
    SELECT
        COUNT(DISTINCT p.sk_patient_id) AS pds_total_sk,
        COUNT(DISTINCT o.sk_patient_id) AS olids_total_sk,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NOT NULL AND o.sk_patient_id IS NOT NULL
            THEN p.sk_patient_id END) AS in_both,
        COUNT(DISTINCT CASE WHEN p.sk_patient_id IS NOT NULL AND o.sk_patient_id IS NULL
            THEN p.sk_patient_id END) AS pds_only,
        COUNT(DISTINCT CASE WHEN o.sk_patient_id IS NOT NULL AND p.sk_patient_id IS NULL
            THEN o.sk_patient_id END) AS olids_only
    FROM (SELECT DISTINCT sk_patient_id FROM {{ ref('stg_pds_patient_care_practice') }} WHERE sk_patient_id IS NOT NULL) p
    FULL OUTER JOIN (SELECT DISTINCT sk_patient_id FROM {{ ref('stg_olids_patient') }} WHERE sk_patient_id IS NOT NULL) o
        ON p.sk_patient_id = o.sk_patient_id
)

SELECT 'OLIDS current registrations' AS metric,
    total_current AS val1, with_sk AS val2, without_sk AS val3, distinct_persons AS val4, distinct_sk AS val5
FROM olids_sk_coverage
UNION ALL
SELECT 'OLIDS patient table',
    total_patients, with_sk, without_sk, distinct_sk, NULL
FROM olids_patient_sk
UNION ALL
SELECT 'sk_patient_id overlap (all records)',
    in_both, pds_only, olids_only, pds_total_sk, olids_total_sk
FROM sk_overlap
