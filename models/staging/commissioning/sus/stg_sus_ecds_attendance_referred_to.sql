{{
    config(materialized = 'view')
}}

select
    primarykey_id
    , referred_to_id
    , rownumber_id
    , service as referred_to_service_code
    , assessment_date::date as assessment_date
    , assessment_time::time as assessment_time
from {{ ref('raw_sus_ecds_attendance_referred_to') }}
