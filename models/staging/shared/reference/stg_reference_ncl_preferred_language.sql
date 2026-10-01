select
    code,
    preferred_language
from {{ ref('raw_reference_lookup_ncl_preferred_language') }}
