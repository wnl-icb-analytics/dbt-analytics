select
    sk_unit_id
    , nullif(trim(unit_label), '') as unit_label
from {{ ref('raw_dictionary_dbo_unitmapping') }}
