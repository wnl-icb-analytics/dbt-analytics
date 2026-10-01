{{ config(materialized = 'table') }}

select
    bk_rtt_period_status_code::varchar as rtt_period_status_code
    , rtt_period_status_category
    , rtt_period_status_description
    , notes
from {{ ref('raw_dictionary_dbo_rttperiodstatus') }}
where bk_rtt_period_status_code is not null
qualify row_number() over (
    partition by bk_rtt_period_status_code
    order by sk_rtt_period_status_id desc
) = 1
