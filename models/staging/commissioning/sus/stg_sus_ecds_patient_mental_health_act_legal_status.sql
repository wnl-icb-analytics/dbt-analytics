{{
    config(materialized = 'view')
}}

select
    primarykey_id
    , mental_health_act_legal_status_id
    , rownumber_id
    , classification as legal_status_code
    , assignment_timestamp::timestamp_tz as assigned_at
    -- Older ECDS deliveries supply the assignment start as separate date and time.
    , start_date::date as start_date
    , try_to_time(start_time) as start_time
    , expiry_timestamp::timestamp_tz as expires_at
from {{ ref('raw_sus_ecds_patient_mental_health_act_legal_status') }}
