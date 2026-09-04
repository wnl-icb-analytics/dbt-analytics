{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'])
}}

-- Community services activity block for segmentation. Grain: one row per
-- sk_patient_id with any attended CSDS care contact in the rolling 12
-- months ending on the latest contact date (lag-aware).
--
-- community_contacts_excluding_health_visiting_12mo excludes Health Visiting
-- Service contacts (team type 16) for use by the child complexity criterion.
-- The general contact count is retained for other consumers. Some CSDS records
-- carry no sk_patient_id, so contact counts are a floor; sk_patient_id '1' is a
-- shared junk key and is excluded.

WITH cc_max_date AS (
    SELECT MAX(care_contact_date) AS max_date
    FROM {{ ref('int_csds_contact_currency') }}
    WHERE
        care_contact_date <= CURRENT_DATE()
        AND attendance_status IN ('5', '6')
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
)

SELECT
    c.sk_patient_id,
    COUNT(*) AS community_contacts_12mo,
    COUNT_IF(COALESCE(c.team_type_code, '') != '16')
        AS community_contacts_excluding_health_visiting_12mo
FROM {{ ref('int_csds_contact_currency') }} AS c
CROSS JOIN cc_max_date AS m
WHERE
    c.care_contact_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
    AND c.attendance_status IN ('5', '6')
    AND c.sk_patient_id IS NOT NULL
    AND c.sk_patient_id != '1'
GROUP BY c.sk_patient_id
