-- PDS Data Overview
-- Explores PDS table shapes, date ranges, and basic counts
-- to understand the PDS data available for comparison
--
-- Usage: dbt compile -s pds_data_overview

-- 1. PDS care practice records: total, current, and date range
WITH care_practice_stats AS (
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT sk_patient_id) AS distinct_patients,
        COUNT(DISTINCT practice_code) AS distinct_practices,
        COUNT(CASE WHEN CURRENT_DATE() BETWEEN event_from_date
            AND COALESCE(event_to_date, '9999-12-31') THEN 1 END) AS current_records,
        COUNT(DISTINCT CASE WHEN CURRENT_DATE() BETWEEN event_from_date
            AND COALESCE(event_to_date, '9999-12-31') THEN sk_patient_id END) AS current_patients,
        COUNT(DISTINCT CASE WHEN CURRENT_DATE() BETWEEN event_from_date
            AND COALESCE(event_to_date, '9999-12-31') THEN practice_code END) AS current_practices,
        MIN(event_from_date) AS earliest_from,
        MAX(event_from_date) AS latest_from,
        MIN(event_to_date) AS earliest_to,
        MAX(event_to_date) AS latest_to,
        COUNT(CASE WHEN event_to_date IS NULL THEN 1 END) AS null_end_date_count
    FROM {{ ref('stg_pds_patient_care_practice') }}
),

-- 2. PDS person records: deceased and death status breakdown
person_stats AS (
    SELECT
        COUNT(*) AS total_person_records,
        COUNT(DISTINCT sk_patient_id) AS distinct_persons,
        COUNT(CASE WHEN death_status IS NOT NULL THEN 1 END) AS with_death_status,
        COUNT(CASE WHEN date_of_death IS NOT NULL THEN 1 END) AS with_death_date,
        COUNT(CASE WHEN death_status IS NULL AND date_of_death IS NULL THEN 1 END) AS alive_records,
        COUNT(DISTINCT CASE WHEN CURRENT_DATE() BETWEEN event_from_date
            AND COALESCE(event_to_date, '9999-12-31') THEN sk_patient_id END) AS current_person_records
    FROM {{ ref('stg_pds_person') }}
),

-- 3. Reason for removal breakdown
removal_stats AS (
    SELECT
        COUNT(*) AS total_removal_records,
        COUNT(DISTINCT sk_patient_id) AS patients_with_removal,
        COUNT(CASE WHEN CURRENT_DATE() BETWEEN event_from_date
            AND COALESCE(event_to_date, '9999-12-31') THEN 1 END) AS current_removals
    FROM {{ ref('stg_pds_reason_for_removal') }}
),

-- 4. Person merger stats
merger_stats AS (
    SELECT
        COUNT(*) AS total_mergers,
        COUNT(DISTINCT sk_patient_id) AS merged_from,
        COUNT(DISTINCT sk_patient_id_superseded) AS merged_to
    FROM {{ ref('stg_pds_person_merger') }}
)

SELECT 'care_practice' AS table_name, * FROM care_practice_stats
UNION ALL
SELECT 'person', total_person_records, distinct_persons, with_death_status,
    with_death_date, alive_records, current_person_records, NULL, NULL, NULL, NULL, NULL
FROM person_stats
UNION ALL
SELECT 'removal', total_removal_records, patients_with_removal, current_removals,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM removal_stats
UNION ALL
SELECT 'merger', total_mergers, merged_from, merged_to,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM merger_stats
