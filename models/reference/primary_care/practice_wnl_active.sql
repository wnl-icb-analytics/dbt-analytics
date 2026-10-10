{{ config(materialized='view') }}

select
    practice_code
    , practice_name
    , practice_status
    , open_date
    , close_date
    , sub_icb_code
    , sub_icb_name
    , health_borough_name
    , registered_borough_name
    , geographic_borough_name
    , pcn_code
    , pcn_name
    , pcn_name_with_borough
    , neighbourhood_code
    , neighbourhood_code_legacy
    , neighbourhood_name
    , neighbourhood_name_with_borough
    , is_isa_accepted
    , postcode
    , address_line_1
    , address_line_2
    , address_line_3
    , address_line_4
    , address_line_5
    , contact_phone
    , uprn
    , latitude
    , longitude
    , lsoa_code
    , msoa_code
    , ods_first_created_at
    , ods_last_updated_at
    , details_valid_from
from {{ ref('practice_wnl_all') }}
where practice_status = 'Active'
