{{ config(materialized='view') }}

-- Legacy column names for consumers of the former app view. Use pcn_wnl_active.
select
    pcn_code
    , pcn_name
    , pcn_name_with_borough
    , sub_icb_code
    , health_borough_name
    , registered_borough_name
    , open_date
    , pcn_status
    , active_member_practice_count
from {{ ref('pcn_wnl_active') }}
