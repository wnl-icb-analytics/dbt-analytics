{{
    config(
        materialized='table'
    )
}}

/*
Geographic Code to Name Mappings
Provides a single source of truth for all geography code-to-name mappings.
Includes ICBs, local authorities, LSOAs, MSOAs and their hierarchies.
*/

WITH postcode_geography AS (
    -- REVIEW: Dictionary.dbo.Postcode replaces the hashed OLIDS join source.
    SELECT DISTINCT
        primary_care_organisation,
        local_authority_district_unitary_authority as local_authority_organisation,
        yr2011_lsoa as yr_2011_lsoa,
        yr2011_msoa as yr_2011_msoa,
        lsoa as yr_2021_lsoa, -- REVIEW: generic current LSOA is treated as the 2021 code.
        msoa as yr_2021_msoa -- REVIEW: generic current MSOA is treated as the 2021 code.
    FROM {{ ref('stg_dictionary_dbo_postcode') }}
    WHERE postcode_no_space IS NOT NULL
),

primary_care_orgs AS (
    -- ICB/Primary Care Organisation mappings with cleaned names
    SELECT DISTINCT
        organisation_code,
        -- Remove organisation code suffix from names
        CASE
            WHEN organisation_name LIKE '% - ' || organisation_code
                THEN TRIM(REGEXP_REPLACE(organisation_name, ' - ' || organisation_code || '$', ''))
            WHEN organisation_name RLIKE '.* - [A-Z0-9]+$'
                THEN TRIM(REGEXP_REPLACE(organisation_name, ' - [A-Z0-9]+$', ''))
            ELSE organisation_name
        END AS organisation_name,
        start_date,
        end_date,
        'PRIMARY_CARE' AS org_type
    FROM {{ ref('stg_dictionary_dbo_organisation') }}
    WHERE organisation_code IN (SELECT DISTINCT primary_care_organisation FROM postcode_geography)
        AND (end_date IS NULL OR end_date >= CURRENT_DATE())
),

local_authority_orgs AS (
    -- Local Authority/Borough mappings
    SELECT DISTINCT
        organisation_code,
        organisation_name,
        start_date,
        end_date,
        'LOCAL_AUTHORITY' AS org_type
    FROM {{ ref('stg_dictionary_dbo_organisation') }}
    WHERE organisation_code IN (SELECT DISTINCT local_authority_organisation FROM postcode_geography)
        AND (end_date IS NULL OR end_date >= CURRENT_DATE())
),

lsoa_2021_names AS (
    -- LSOA 2021 names
    SELECT DISTINCT
        lsoa21_cd AS geography_code,
        lsoa21_nm AS geography_name,
        'LSOA_2021' AS geography_type
    FROM {{ ref('stg_reference_lsoa2011_lsoa2021') }}
    WHERE lsoa21_cd IS NOT NULL
        AND lsoa21_nm IS NOT NULL
),

lsoa_2011_names AS (
    -- LSOA 2011 names
    SELECT DISTINCT
        lsoa11_cd AS geography_code,
        lsoa11_nm AS geography_name,
        'LSOA_2011' AS geography_type
    FROM {{ ref('stg_reference_lsoa2011_lsoa2021') }}
    WHERE lsoa11_cd IS NOT NULL
        AND lsoa11_nm IS NOT NULL
),

neighbourhood_mappings AS (
    -- NCL Neighbourhood mappings
    SELECT DISTINCT
        lsoa_2021_code AS geography_code,
        neighbourhood_name AS mapped_value,
        'LSOA_TO_NEIGHBOURHOOD' AS mapping_type
    FROM {{ ref('stg_reference_ncl_neighbourhood_lsoa_2021') }}
    WHERE lsoa_2021_code IS NOT NULL
        AND neighbourhood_name IS NOT NULL
),

all_organisations AS (
    -- Combine all organisation types
    SELECT * FROM primary_care_orgs
    UNION ALL
    SELECT * FROM local_authority_orgs
),

all_geographies AS (
    -- Combine all geography types
    SELECT * FROM lsoa_2021_names
    UNION ALL
    SELECT * FROM lsoa_2011_names
)

-- Final output combining organisations and geographies
SELECT
    'ORGANISATION' AS mapping_category,
    org.org_type AS mapping_type,
    org.organisation_code AS code,
    org.organisation_name AS name,
    org.start_date,
    org.end_date,
    NULL AS parent_code,
    NULL AS parent_name
FROM all_organisations org

UNION ALL

SELECT
    'GEOGRAPHY' AS mapping_category,
    geo.geography_type AS mapping_type,
    geo.geography_code AS code,
    geo.geography_name AS name,
    NULL AS start_date,
    NULL AS end_date,
    NULL AS parent_code,
    NULL AS parent_name
FROM all_geographies geo

UNION ALL

SELECT
    'GEOGRAPHY_MAPPING' AS mapping_category,
    nbhd.mapping_type,
    nbhd.geography_code AS code,
    nbhd.mapped_value AS name,
    NULL AS start_date,
    NULL AS end_date,
    NULL AS parent_code,
    nbhd.mapped_value AS parent_name
FROM neighbourhood_mappings nbhd
