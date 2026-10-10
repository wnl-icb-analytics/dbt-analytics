{{ config(materialized='view') }}

select
    practice_code
    , practice_name
    , practice_status
    , open_date
    , close_date
    , sub_icb_code
    , sub_icb_name
    , icb_code
    , icb_name
    , region_code
    , region_name
    , pcn_code
    , pcn_name
    , is_wnl_practice
    , postcode
    , address_line_1
    , address_line_2
    , address_line_3
    , address_line_4
    , address_line_5
    , contact_phone
    , details_valid_from
from {{ ref('practice_national_all') }}
where practice_status = 'Active'
