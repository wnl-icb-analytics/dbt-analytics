{{
    config(materialized = 'view')
}}

select
    primarykey_id
    , expected_treatment_times_id
    , rownumber_id
    , timestamp::timestamp_tz as expected_treatment_at
    , allocated_timestamp::timestamp_tz as allocated_at
from {{ ref('raw_sus_ecds_attendance_expected_treatment_times') }}
