{{ config(materialized='view') }}

-- Legacy column names for consumers of the former app view. Use neighbourhood_wnl_active.
select
    neighbourhood_code
    , neighbourhood_code_legacy
    , neighbourhood_name
    , neighbourhood_name_with_borough
    , health_borough_name
    , registered_borough_name
    , sub_icb_code
    , active_member_practice_count
from {{ ref('neighbourhood_wnl_active') }}
