{{
    config(
        materialized='table',
        tags=['cltcs'])
}}

{% set in_scope_borough_list = ['Haringey', 'Camden'] %}
{% set in_scope_neighbourhood_list = [] %}

with in_scope_practice_list as (
    select local_authority, practice_code, neighbourhood_code, neighbourhood_registered
    from {{ ref('cltcs_practice_neighbourhood') }}
    where false
    {% if in_scope_borough_list %}
        or local_authority in ('{{ in_scope_borough_list | join("', '") }}')
    {% endif %}
    {% if in_scope_neighbourhood_list %}
        or neighbourhood_registered in ('{{ in_scope_neighbourhood_list | join("', '") }}')
    {% endif %}
)

select 
    -- practice details
      ip.practice_code
    , dp.practice_name
    , dp.borough_registered

    -- area details
    , ip.neighbourhood_code as area_code
    , ip.neighbourhood_registered as area_name
    , ip.local_authority as area_borough
    -- geography
    , dp.practice_msoa
    , dp.practice_latitude
    , dp.practice_longitude
    
from in_scope_practice_list ip 
left join {{ref('dim_practice')}} dp on dp.practice_code = ip.practice_code