select
    pcn_code
    , pcn_name
    , pcn_name_with_borough
    , pcn_status
    , open_date
    , close_date
    , sub_icb_code
    , {{ wnl_health_borough_name('health_borough_name', 'registered_borough_name') }} as health_borough_name
    , registered_borough_name
    , valid_from
    , valid_to
    , valid_to is null as is_current
    , source_system
    , change_reason
    , created_by
    , created_at
from {{ ref('raw_reference_primary_care_pcn_history') }}
