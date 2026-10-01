{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'organisation'],
        cluster_by=['practice_code'])
}}

/*
Practice Dimension
Contains comprehensive practice details including organisational hierarchy.
Sourced from Dictionary's OrganisationMatrixPracticeView combined with OLIDS organisation data.
Note: Deduplicates by organisation_id to ensure uniqueness when multiple practice codes map to same organisation.
*/

WITH practice_org_joined AS (
    SELECT
    -- OLIDS identifiers
    org.id AS organisation_id,
    
    -- Practice details
    dict.practice_code AS practice_code,
    {{ clean_practice_name('dict_org.organisation_name', 'dict.practice_name') }} AS practice_name,
    
    -- PCN details
    dict.network_code AS pcn_code,
    dict.network_name AS pcn_name,
    -- PCN name with borough prefix
    CASE 
        WHEN borough_map.pcn_borough IS NOT NULL 
        THEN borough_map.pcn_borough || ': ' || dict.network_name
        ELSE dict.network_name
    END AS pcn_name_with_borough,
    
    -- Borough information
    borough_map.borough_registered,
    borough_map.pcn_borough,
    borough_map.practice_historic_ccg,
    borough_map.sub_icb_code,
    borough_map.sub_icb_name,
    borough_map.legacy_icb_code,
    borough_map.legacy_icb_name,

    -- Practice organisational details from OLIDS
    -- type_code / type_desc were removed in the 2026 OLIDS schema realignment
    -- (issue #747). The replacement free-text `description` is exposed here as
    -- practice_type_desc to preserve the downstream column name; practice_type_code
    -- is no longer available from OLIDS.
    CAST(NULL AS VARCHAR) AS practice_type_code,
    org.description AS practice_type_desc,
    org.postcode AS practice_postcode,
    org.open_date AS practice_open_date,
    org.close_date AS practice_close_date,
    org.is_obsolete AS practice_is_obsolete,
    org.parent_organisation_id AS practice_parent_organisation_id,
    
    -- Enhanced practice details from Dictionary Organisation
    dict_org.start_date AS practice_start_date,
    dict_org.end_date AS practice_end_date,
    dict_org.status AS practice_status,
    -- Operationally active when ODS marks the practice Active and the end date has not passed
    (COALESCE(dict_org.status, 'Unknown') = 'Active'
        AND (dict_org.end_date IS NULL OR dict_org.end_date > CURRENT_DATE())) AS is_active_practice,
    dict_org.address_line_1 AS practice_address_line_1,
    dict_org.address_line_2 AS practice_address_line_2,
    dict_org.address_line_3 AS practice_address_line_3,
    dict_org.address_line_4 AS practice_address_line_4,
    dict_org.address_line_5 AS practice_address_line_5,
    dict_org.first_created AS practice_first_created,
    dict_org.last_updated AS practice_last_updated,
    
    -- Geographic details from Dictionary Postcode
    REGEXP_REPLACE(dict_pc.postcode, '\\s+', ' ') AS practice_postcode_dict,
    dict_pc.lsoa AS practice_lsoa,
    dict_pc.msoa AS practice_msoa,
    dict_pc.latitude AS practice_latitude,
    dict_pc.longitude AS practice_longitude,
    
    -- Commissioner relationship
    dict.commissioner_code AS commissioner_code,
    dict.commissioner_name AS commissioner_name,
    dict.sk_organisation_id_commissioner AS sk_commissioner_id,
    
    -- ICB relationship (top-level; Dictionary still labels the column "STP" upstream)
    dict.stp_code AS icb_code,
    dict.stp_name AS icb_name,
    dict.sk_organisation_id_stp AS sk_icb_id,
    -- WNL = merged ICB (Z9B2Z) plus legacy NCL (QMJ) and NWL (QRV) codes still in use
    (dict.stp_code IN ('Z9B2Z', 'QMJ', 'QRV')) AS is_wnl_practice,
    
    -- Dictionary surrogate keys
    dict.sk_organisation_id_practice AS sk_practice_id,
    dict_org.sk_organisation_id AS sk_practice_dict_id

FROM (
    -- Deduplicate dictionary practices in case of multiple rows per practice code
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY practice_code 
                ORDER BY 
                    sk_organisation_id_practice DESC  -- Use SK as tiebreaker for consistent results
            ) AS dict_rn
        FROM {{ ref('stg_dictionary_dbo_organisationmatrixpracticeview') }}
        WHERE practice_code IS NOT NULL
    ) AS dict_ranked
    WHERE dict_rn = 1
) AS dict
INNER JOIN (
    -- Deduplicate organisations by taking the most recent record per organisation_code
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY organisation_code 
                ORDER BY 
                    CASE WHEN is_obsolete = FALSE THEN 0 ELSE 1 END,  -- Prefer active records
                    lds_transform_datetime DESC,  -- Then most recent data
                    id DESC  -- Finally by ID as tiebreaker
            ) AS rn
        FROM {{ ref('stg_olids_organisation') }}
        WHERE organisation_code IS NOT NULL
    ) AS org_ranked
    WHERE rn = 1
) AS org
    ON dict.practice_code = org.organisation_code
LEFT JOIN (
    SELECT
        organisation_code,
        organisation_name,
        start_date,
        end_date,
        status,
        address_line_1,
        address_line_2,
        address_line_3,
        address_line_4,
        address_line_5,
        first_created,
        last_updated,
        sk_postcode_id,
        sk_organisation_id
    FROM {{ ref('stg_dictionary_dbo_organisation') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY organisation_code ORDER BY organisation_code) = 1
) AS dict_org
    ON dict.practice_code = dict_org.organisation_code
LEFT JOIN (
    SELECT
        sk_postcode_id,
        postcode,
        lsoa,
        msoa,
        latitude,
        longitude
    FROM {{ ref('stg_dictionary_dbo_postcode') }}
) AS dict_pc
    ON dict_org.sk_postcode_id = dict_pc.sk_postcode_id
LEFT JOIN {{ ref('int_organisation_borough_mapping') }} AS borough_map
    ON dict.practice_code = borough_map.practice_code
WHERE dict.practice_code IS NOT NULL
    AND dict.stp_code IN (
        'Z9B2Z', -- NHS WEST AND NORTH LONDON INTEGRATED CARE BOARD (merged NCL + NWL from Apr 2026)
        'QMJ',   -- NHS NORTH CENTRAL LONDON (legacy, retained for historical continuity)
        'QRV',   -- NHS NORTH WEST LONDON (legacy, retained for historical continuity)
        'QMF',   -- NHS NORTH EAST LONDON INTEGRATED CARE BOARD
        'QWE',   -- NHS SOUTH WEST LONDON INTEGRATED CARE BOARD
        'QKK'    -- NHS SOUTH EAST LONDON INTEGRATED CARE BOARD
    )
)

-- Final deduplication by organisation_id to ensure uniqueness
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY organisation_id 
            ORDER BY 
                practice_code  -- Take the first practice code alphabetically for consistency
        ) AS final_rn
    FROM practice_org_joined
) AS deduplicated
WHERE final_rn = 1
