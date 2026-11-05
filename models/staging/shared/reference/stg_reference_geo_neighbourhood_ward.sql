-- WNL electoral ward 2025 -> neighbourhood (INT) from REFERENCE.GEO.NEIGHBOURHOOD_WARD.
-- The ward assignment defines each neighbourhood; NEIGHBOURHOOD_LSOA and the
-- boundaries are derived from it.
select
    ward_code,
    ward_name,
    neighbourhood_code,
    neighbourhood_name
from {{ ref('raw_reference_geo_neighbourhood_ward') }}
