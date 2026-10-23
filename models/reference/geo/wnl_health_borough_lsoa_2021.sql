{{ config(materialized = 'table') }}

with ccg as (
    select
        org.organisation_code as ccg_code
        , org.organisation_name as ccg_name
        , case org.organisation_code
            when '07M' then 'Barnet'
            when '07R' then 'Camden'
            when '07X' then 'Enfield'
            when '08D' then 'Haringey'
            when '08H' then 'Islington'
            when '07P' then 'Brent'
            when '07W' then 'Ealing'
            when '07Y' then 'Hounslow'
            when '08C' then 'Hammersmith & Fulham'
            when '08E' then 'Harrow'
            when '08G' then 'Hillingdon'
            when '08Y' then 'West London'
            when '09A' then 'Central London'
          end as health_borough_name
        -- The source geometry is British National Grid but has no declared SRID.
        , st_setsrid(ccg_map.geom, 27700) as boundary
    from {{ ref('stg_dictionary_geo_os_ccg_map') }} as ccg_map
    inner join {{ ref('stg_dictionary_dbo_organisation') }} as org
        on ccg_map.sk_organisation_id = org.sk_organisation_id
    where org.organisation_code in (
        '07M', '07R', '07X', '08D', '08H', '07P', '07W'
        , '07Y', '08C', '08E', '08G', '08Y', '09A'
    )
    qualify row_number() over (
        partition by org.organisation_code
        order by ccg_map.date_updated desc nulls last, ccg_map.date_created desc nulls last
    ) = 1
),

lsoa as (
    select
        lsoa_2021_code
        , st_transform(
            to_geometry(st_makepoint(longitude::float, latitude::float), 4326)
            , 27700
          ) as centroid
    from {{ ref('stg_reference_geo_lsoa_21_wnl') }}
)

select
    lsoa.lsoa_2021_code
    , ccg.ccg_code
    , ccg.ccg_name
    , ccg.health_borough_name
from lsoa
inner join ccg
    on st_contains(ccg.boundary, lsoa.centroid)
