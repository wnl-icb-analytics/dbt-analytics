select
    *
from {{ source('reference_analyst_managed', 'CSDS_LOOKUP') }}
