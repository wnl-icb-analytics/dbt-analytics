select
    sk_read_code_id
    , read_code
    , term
    , snomed_ct_code
    , is_read_v2
    , is_ctv3
    , read_code_alt
    , date_created
    , date_updated
from {{ ref('raw_dictionary_dbo_readcodes') }}
