select
    lsoa_2021_code,
    lsoa_2021_name,
    neighbourhood_code,
    neighbourhood_name,
    cast(start_date as date) as start_date
from {{ ref('raw_reference_lookup_ncl_ncl_neighbourhood_lsoa_2021') }}
