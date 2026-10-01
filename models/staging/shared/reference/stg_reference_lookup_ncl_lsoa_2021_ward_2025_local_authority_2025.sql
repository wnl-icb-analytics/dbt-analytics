select
    lsoa_2021_code,
    lsoa_2021_name,
    ward_2025_code,
    ward_2025_name,
    local_authority_2025_code,
    local_authority_2025_name,
    icb_code,
    icb_name,
    -- Source resident_flag tags NCL but lumps NWL into 'Other London'. Derive
    -- 'NWL' from the ICB name so the WNL (NCL + NWL) in-area population can be
    -- flagged consistently. NCL and all other values pass through unchanged.
    case
        when icb_name ilike '%North West London%' then 'NWL'
        else resident_flag
    end as resident_flag
from {{ ref('raw_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025') }}
