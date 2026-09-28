{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.POSTCODE_HASH \ndbt: source(''olids'', ''POSTCODE_HASH'') \nColumns:\n  POSTCODE -> postcode\n  POSTCODE_HASH -> postcode_hash\n  OUTCODE -> outcode\n  LOCAL_AUTHORITY_ORGANISATION -> local_authority_organisation\n  PRIMARY_CARE_ORGANISATION -> primary_care_organisation\n  YR2011_LSOA -> yr2011_lsoa\n  YR2021_LSOA -> yr2021_lsoa\n  YR2011_MSOA -> yr2011_msoa\n  YR2021_MSOA -> yr2021_msoa\n  WARD -> ward\n  VERSION -> version\n  LAST_UPDATED -> last_updated"
    )
}}
select
    "POSTCODE" as postcode,
    "POSTCODE_HASH" as postcode_hash,
    "OUTCODE" as outcode,
    "LOCAL_AUTHORITY_ORGANISATION" as local_authority_organisation,
    "PRIMARY_CARE_ORGANISATION" as primary_care_organisation,
    "YR2011_LSOA" as yr2011_lsoa,
    "YR2021_LSOA" as yr2021_lsoa,
    "YR2011_MSOA" as yr2011_msoa,
    "YR2021_MSOA" as yr2021_msoa,
    "WARD" as ward,
    "VERSION" as version,
    "LAST_UPDATED" as last_updated
from {{ source('olids', 'POSTCODE_HASH') }}
