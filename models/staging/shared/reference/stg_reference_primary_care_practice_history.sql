select
    practice_code
    , practice_name
    , practice_status
    , prescribing_setting
    , postcode
    , open_date
    , close_date
    , sub_icb_code
    , sub_icb_name
    , {{ wnl_health_borough_name('health_borough_name', 'registered_borough_name') }} as health_borough_name
    , registered_borough_name
    , geographic_borough_name
    , isa_accepted as is_isa_accepted
    , uprn
    , latitude
    , longitude
    , lsoa as lsoa_code
    , msoa as msoa_code
    , address_line_1
    , address_line_2
    , address_line_3
    , address_line_4
    , address_line_5
    , contact_phone
    , ods_first_created as ods_first_created_at
    , ods_last_updated as ods_last_updated_at
    , valid_from
    , valid_to
    , valid_to is null as is_current
    , source_system
    , change_reason
    , created_by
    , created_at
from {{ ref('raw_reference_primary_care_practice_history') }}
