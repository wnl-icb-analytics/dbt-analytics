-- WNL residence neighbourhood: LSOA 2021 -> neighbourhood, all 13 WNL boroughs
-- (NCL + NWL). Source is name-only ("INT"); no neighbourhood code.
select
    lsoa21_cd as lsoa_2021_code,
    lsoa21_nm as lsoa_2021_name,
    laname as borough,
    nullif(trim("INT"), '') as neighbourhood_name,
    imd25_decil as imd_2025_decile
from {{ ref('raw_reference_wnlneighbourhoods') }}
