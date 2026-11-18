{{ config(materialized='view') }}

with neighbourhood as (
    select
        neighbourhood_code
        , neighbourhood_code_legacy
        , neighbourhood_name
        , neighbourhood_name_with_borough
        , health_borough_name
        , registered_borough_name
        , sub_icb_code
    from {{ ref('stg_reference_primary_care_neighbourhood') }}
    where is_active
)

, member_counts as (
    select neighbourhood_code, count(*) as active_member_practice_count
    from {{ ref('practice_wnl_active') }}
    where neighbourhood_code is not null
    group by neighbourhood_code
)

select
    neighbourhood.neighbourhood_code
    , neighbourhood.neighbourhood_code_legacy
    , neighbourhood.neighbourhood_name
    , neighbourhood.neighbourhood_name_with_borough
    , neighbourhood.health_borough_name
    , neighbourhood.registered_borough_name
    , neighbourhood.sub_icb_code
    , coalesce(member_counts.active_member_practice_count, 0) as active_member_practice_count
from neighbourhood
left join member_counts
    on neighbourhood.neighbourhood_code = member_counts.neighbourhood_code
