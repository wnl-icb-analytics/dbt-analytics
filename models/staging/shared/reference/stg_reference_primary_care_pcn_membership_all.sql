{{
    config(materialized = 'table')
}}

select
    practice_code
    , practice_name
    , practice_status
    , pcn_code
    , pcn_name
    , health_borough_name
    , registered_borough_name
from {{ ref('raw_reference_primary_care_pcn_membership_all') }}
