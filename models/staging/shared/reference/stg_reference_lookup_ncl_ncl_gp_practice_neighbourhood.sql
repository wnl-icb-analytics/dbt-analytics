select
    practice_code,
    practice_name,
    neighbourhood_code,
    neighbourhood_name
from {{ ref('raw_reference_lookup_ncl_ncl_gp_practice_neighbourhood') }}