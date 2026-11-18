select
    practice_code
    , pcn_code
    , pcn_name
    , sub_icb_code
    , valid_from
    , valid_to
    , valid_to is null as is_current
    , source_system
    , ods_relationship_id
    , change_reason
    , created_by
    , created_at
from {{ ref('raw_reference_primary_care_practice_pcn_history') }}
