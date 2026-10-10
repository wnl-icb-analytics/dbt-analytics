{{
    config(
        materialized='table',
        tags=['cltcs', 'daily'])
}}

select patient_id
    , area_code
    , intervention_date
    , action
    , action_date
    , detail
    , intervention_name
from {{ ref('stg_c_ltcs_status_log') }}
qualify row_number() over (partition by patient_id order by action_date desc) = 1