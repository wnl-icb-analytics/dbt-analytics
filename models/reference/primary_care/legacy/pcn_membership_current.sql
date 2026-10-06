{{ config(materialized='view') }}

-- Legacy column names for consumers of the former app view. Use practice_wnl_active.
select
    pcn_code
    , pcn_name
    , pcn_name_with_borough
    , practice_code
    , practice_name
    , practice_status
    , sub_icb_code
    , health_borough_name
    , registered_borough_name
from {{ ref('practice_wnl_active') }}
