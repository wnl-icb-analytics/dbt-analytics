{{ config(materialized='view') }}

-- Legacy column names for consumers of the former app view. Use practice_wnl_all.
select
    practice_code
    , practice_name
    , practice_status
    , postcode
    , open_date
    , close_date
    , sub_icb_code
    , sub_icb_name
    , health_borough_name
    , registered_borough_name
    , geographic_borough_name
    , is_isa_accepted as isa_accepted
    , pcn_code
    , pcn_name
    , pcn_name_with_borough
    , neighbourhood_code
    , neighbourhood_name
    , neighbourhood_name_with_borough
    , uprn
    , latitude
    , longitude
    , lsoa_code as lsoa
    , msoa_code as msoa
    , contact_phone
    , address_line_1
    , address_line_2
    , address_line_3
    , address_line_4
    , address_line_5
    , ods_first_created_at as ods_first_created
    , ods_last_updated_at as ods_last_updated
    , details_valid_from as details_since
from {{ ref('practice_wnl_all') }}
