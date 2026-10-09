{{ config(materialized = 'table') }}

-- WNL health boroughs retain the former CCG geography and names. They are not
-- the London local authority classification exposed by the source lookup.
with lsoa_health_borough as (
    select
        lsoa21_cd as lsoa_2021_code
        , {{ wnl_health_borough_name('null', 'lad26_nm') }} as health_borough_name
    from {{ ref('stg_reference_geo_lsoa21_sicbl26_icb26_nhser26_lad26') }}
)

select
    lsoa_2021_code
    , health_borough_name
from lsoa_health_borough
where health_borough_name in (
    'Brent'
    , 'Central London'
    , 'Ealing'
    , 'Hammersmith & Fulham'
    , 'Harrow'
    , 'Hillingdon'
    , 'Hounslow'
    , 'West London'
)
