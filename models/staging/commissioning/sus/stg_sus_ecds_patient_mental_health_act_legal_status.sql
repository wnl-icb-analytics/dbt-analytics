{{
    config(materialized = 'view')
}}

select
    primarykey_id
    , mental_health_act_legal_status_id
    , rownumber_id
    , classification as legal_status_code
    , assignment_timestamp::timestamp_tz as assigned_at
    , expiry_timestamp::timestamp_tz as expires_at
from {{ ref('raw_sus_ecds_patient_mental_health_act_legal_status') }}
