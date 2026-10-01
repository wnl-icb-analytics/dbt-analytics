select
    code,
    preferred_language,
    iso_origin
from {{ ref('raw_reference_lookup_ncl_preferred_language') }}
