{{ config(materialized='view') }}

with pcn as (
    select
        pcn_code
        , pcn_name
        , pcn_name_with_borough
        , pcn_status
        , open_date
        , sub_icb_code
        , health_borough_name
        , registered_borough_name
    from {{ ref('stg_reference_primary_care_pcn_history') }}
    where is_current
        and pcn_status = 'Active'
)

, member_counts as (
    select pcn_code, count(*) as active_member_practice_count
    from {{ ref('practice_wnl_active') }}
    where pcn_code is not null
    group by pcn_code
)

select
    pcn.pcn_code
    , pcn.pcn_name
    , pcn.pcn_name_with_borough
    , pcn.pcn_status
    , pcn.open_date
    , pcn.sub_icb_code
    , pcn.health_borough_name
    , pcn.registered_borough_name
    , coalesce(member_counts.active_member_practice_count, 0) as active_member_practice_count
from pcn
left join member_counts
    on pcn.pcn_code = member_counts.pcn_code
