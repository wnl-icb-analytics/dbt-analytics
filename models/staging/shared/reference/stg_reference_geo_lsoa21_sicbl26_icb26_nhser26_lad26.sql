{{
    config(materialized = 'table')
}}

-- one row per LSOA21: the source repeats LSOAs across boundary versions,
-- lowest objectid wins
select
    lsoa21_cd
    , lsoa21_nm
    , sicbl26_cd
    , sicbl26_cdh
    , sicbl26_nm
    , icb26_cd
    , icb26_cdh
    , icb26_nm
    , lad26_cd
    , lad26_nm
from {{ ref('raw_reference_geo_lsoa21_sicbl26_icb26_nhser26_lad26') }}
where lsoa21_cd is not null
qualify row_number() over (partition by lsoa21_cd order by objectid) = 1
