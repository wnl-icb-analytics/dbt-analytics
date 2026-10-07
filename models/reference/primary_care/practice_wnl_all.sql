{{ config(materialized='view') }}

-- A view so edits made in the primary care org manager app show immediately.
with practice as (
    select *
    from {{ ref('stg_reference_primary_care_practice_history') }}
    where is_current
)

, pcn_membership as (
    select practice_code, pcn_code
    from {{ ref('stg_reference_primary_care_practice_pcn_history') }}
    where is_current
)

, pcn as (
    select pcn_code, pcn_name, pcn_name_with_borough
    from {{ ref('stg_reference_primary_care_pcn_history') }}
    where is_current
)

, neighbourhood_mapping as (
    select practice_code, neighbourhood_code
    from {{ ref('stg_reference_primary_care_practice_neighbourhood_history') }}
    where is_current
)

, neighbourhood as (
    select
        neighbourhood_code
        , neighbourhood_code_legacy
        , neighbourhood_name
        , neighbourhood_name_with_borough
    from {{ ref('stg_reference_primary_care_neighbourhood') }}
)

select
    practice.practice_code
    , practice.practice_name
    , practice.practice_status
    , practice.open_date
    , practice.close_date
    , practice.sub_icb_code
    , practice.sub_icb_name
    , practice.health_borough_name
    , practice.registered_borough_name
    , practice.geographic_borough_name
    , pcn_membership.pcn_code
    , pcn.pcn_name
    , pcn.pcn_name_with_borough
    , neighbourhood_mapping.neighbourhood_code
    , neighbourhood.neighbourhood_code_legacy
    , neighbourhood.neighbourhood_name
    , neighbourhood.neighbourhood_name_with_borough
    , practice.is_isa_accepted
    , practice.postcode
    , practice.address_line_1
    , practice.address_line_2
    , practice.address_line_3
    , practice.address_line_4
    , practice.address_line_5
    , practice.contact_phone
    , practice.uprn
    , practice.latitude
    , practice.longitude
    , practice.lsoa_code
    , practice.msoa_code
    , practice.ods_first_created_at
    , practice.ods_last_updated_at
    , practice.valid_from as details_valid_from
from practice
left join pcn_membership
    on practice.practice_code = pcn_membership.practice_code
left join pcn
    on pcn_membership.pcn_code = pcn.pcn_code
left join neighbourhood_mapping
    on practice.practice_code = neighbourhood_mapping.practice_code
left join neighbourhood
    on neighbourhood_mapping.neighbourhood_code = neighbourhood.neighbourhood_code
