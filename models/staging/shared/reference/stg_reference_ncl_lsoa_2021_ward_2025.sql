select
    lsoa_2021_code,
    lsoa_2021_name,
    ward_2025_code,
    ward_2025_name,
    local_authority_2025_code,
    local_authority_2025_name,
    resident_flag
from {{ ref('raw_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025') }}
