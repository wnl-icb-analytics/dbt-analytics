-- NCL LSOA 2021 -> neighbourhood (INT) from REFERENCE.GEO.NEIGHBOURHOOD_LSOA.
-- Replaces DATA_LAKE__NCL.ANALYST_MANAGED.NCL_NEIGHBOURHOOD_LSOA_2021
-- (identical LSOA coverage, verified 806/806). Codes and names use the
-- canonical REFERENCE scheme (BAR01 / "Barnet: East"), not the legacy N001
-- scheme. start_date dropped: not carried by REFERENCE and no consumers.
select
    lsoa_code as lsoa_2021_code,
    lsoa_name as lsoa_2021_name,
    neighbourhood_code,
    neighbourhood_name
from {{ ref('raw_reference_geo_neighbourhood_lsoa') }}
where sub_icb_code = '93C'
