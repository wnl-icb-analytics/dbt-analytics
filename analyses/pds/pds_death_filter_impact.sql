-- PDS Death Filtering Impact
-- Compares PDS death filtering (death_status/date_of_death)
-- vs OLIDS death filtering (death_date_approx) to quantify the difference
--
-- Usage: dbt compile -s pds_death_filter_impact

WITH ncl_practices AS (
    SELECT prac.organisation_code AS practice_code
    FROM {{ ref('stg_dictionary_dbo_organisation') }} prac
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_organisation_id_parent_org = icb.sk_organisation_id
        AND icb.organisation_code = '93C'
        AND prac.end_date IS NULL
),

-- PDS current registrations (no death filter)
pds_no_death_filter AS (
    SELECT DISTINCT reg.sk_patient_id, reg.practice_code
    FROM {{ ref('stg_pds_patient_care_practice') }} reg
    INNER JOIN ncl_practices np ON reg.practice_code = np.practice_code
    WHERE reg.sk_patient_id IS NOT NULL
        AND CURRENT_DATE() BETWEEN reg.event_from_date
            AND COALESCE(reg.event_to_date, '9999-12-31')
),

-- PDS person death info
pds_person AS (
    SELECT DISTINCT
        sk_patient_id,
        death_status,
        date_of_death
    FROM {{ ref('stg_pds_person') }}
    WHERE CURRENT_DATE() BETWEEN event_from_date
        AND COALESCE(event_to_date, '9999-12-31')
),

-- Classify PDS patients by death status
pds_death_classified AS (
    SELECT
        r.sk_patient_id,
        r.practice_code,
        p.death_status,
        p.date_of_death,
        CASE
            WHEN p.sk_patient_id IS NULL THEN 'no person record'
            WHEN p.death_status IS NOT NULL AND p.date_of_death IS NOT NULL THEN 'deceased (status + date)'
            WHEN p.death_status IS NOT NULL THEN 'deceased (status only)'
            WHEN p.date_of_death IS NOT NULL THEN 'deceased (date only)'
            ELSE 'alive'
        END AS death_category
    FROM pds_no_death_filter r
    LEFT JOIN pds_person p ON r.sk_patient_id = p.sk_patient_id
)

SELECT
    death_category,
    COUNT(DISTINCT sk_patient_id) AS patients,
    COUNT(DISTINCT practice_code) AS across_practices,
    ROUND(100.0 * COUNT(DISTINCT sk_patient_id) /
        SUM(COUNT(DISTINCT sk_patient_id)) OVER (), 2) AS pct
FROM pds_death_classified
GROUP BY death_category
ORDER BY patients DESC
