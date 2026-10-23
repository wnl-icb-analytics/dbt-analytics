{{ config(materialized = 'view') }}

select
    area_code as lsoa_2021_code
    , area_name as lsoa_2021_name
    , latitude
    , longitude
from {{ ref('raw_reference_geo_lsoa_21_wnl') }}
where is_wnl
  and area_code is not null
