-- WNL LSOA 2021 -> neighbourhood (INT) from REFERENCE.GEO.NEIGHBOURHOOD_LSOA,
-- which the org manager app derives from its ward -> neighbourhood lookup.
-- Codes and names use the canonical REFERENCE scheme (BAR01 / "Barnet East").
select
    lsoa_code as lsoa_2021_code,
    lsoa_name as lsoa_2021_name,
    neighbourhood_code,
    neighbourhood_name
from {{ ref('raw_reference_geo_neighbourhood_lsoa') }}
