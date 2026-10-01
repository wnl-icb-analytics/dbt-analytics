{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}


{% set in_scope_borough_list = ['Haringey'] %}
{% set in_scope_neighbourhood_list = ['East Camden'] %}

with in_scope_practice_list as (
    select  local_authority, practice_code, neighbourhood_code, neighbourhood_registered
    from {{ ref('stg_cltcs_emis_cltcs_local_mapping_nh_gp')}}
    where 
    local_authority in (
        {% for b in in_scope_borough_list %}
            '{{ b }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
    or 
    neighbourhood_registered in (
        {% for n in in_scope_neighbourhood_list %}
            '{{ n }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
)

select 
    -- practice details
      ip.practice_code
    , dp.practice_name
    , dp.borough_registered
    , ap.active_patient_count
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
left join {{ref('fct_organisation_active_patients')}} ap on dp.organisation_id = ap.organisation_id