select
    lsoacode,
    imddecile
from {{ ref('raw_reference_imd2019') }}
