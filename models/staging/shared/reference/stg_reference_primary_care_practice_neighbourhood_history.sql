select
    practice_code
    , neighbourhood_code
    , neighbourhood_name
    , sub_icb_code
    , valid_from
    , valid_to
    , valid_to is null as is_current
    , source_system
    , change_reason
    , created_by
    , created_at
from {{ ref('raw_reference_primary_care_practice_neighbourhood_history') }}
