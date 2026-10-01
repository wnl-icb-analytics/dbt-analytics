select
    -- Primary key (compound)
    postcode_hash,

    -- Business columns
    outcode,
    primary_care_organisation,
    local_authority_organisation,
    yr2011_lsoa as yr_2011_lsoa,
    yr2011_msoa as yr_2011_msoa,
    yr2021_lsoa as yr_2021_lsoa,
    yr2021_msoa as yr_2021_msoa,
    ward,
    version,
    last_updated

from {{ ref('raw_olids_postcode_hash') }}
