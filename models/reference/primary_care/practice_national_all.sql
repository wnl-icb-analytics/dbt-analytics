with practice as (
    select
        organisation_code as practice_code
        , organisation_name as practice_name
        , organisation_status as practice_status
        , open_date
        , close_date
        , sub_icb_code
        , icb_code
        , region_code
        , postcode
        , address_line_1
        , address_line_2
        , address_line_3
        , address_line_4
        , address_line_5
        , contact_phone
        , effective_from_at::date as details_valid_from
    from {{ ref('stg_ukhfd_gp_practices_and_prescribing_ccs') }}
    where is_latest
        and is_gp_practice
)

-- Open memberships in the latest file of the latest daily snapshot
, pcn_membership as (
    select practice_code, pcn_code, pcn_name
    from {{ ref('stg_ukhfd_pcn_core_partner_details') }}
    where snapshot_date = (
            select max(snapshot_date)
            from {{ ref('stg_ukhfd_pcn_core_partner_details') }}
        )
        and (relationship_end_date is null or relationship_end_date > snapshot_date)
    qualify row_number() over (
        partition by practice_code
        order by snapshot_file_version desc, relationship_start_date desc, pcn_code
    ) = 1
)

, organisation as (
    select organisation_code, organisation_name
    from {{ ref('organisation') }}
)

, wnl_practice as (
    select practice_code
    from {{ ref('practice_wnl_all') }}
)

select
    practice.practice_code
    , practice.practice_name
    , practice.practice_status
    , practice.open_date
    , practice.close_date
    , practice.sub_icb_code
    , sub_icb.organisation_name as sub_icb_name
    , practice.icb_code
    , icb.organisation_name as icb_name
    , practice.region_code
    , region.organisation_name as region_name
    , pcn_membership.pcn_code
    , pcn_membership.pcn_name
    , wnl_practice.practice_code is not null as is_wnl_practice
    , practice.postcode
    , practice.address_line_1
    , practice.address_line_2
    , practice.address_line_3
    , practice.address_line_4
    , practice.address_line_5
    , practice.contact_phone
    , practice.details_valid_from
from practice
left join pcn_membership
    on practice.practice_code = pcn_membership.practice_code
left join organisation as sub_icb
    on practice.sub_icb_code = sub_icb.organisation_code
left join organisation as icb
    on practice.icb_code = icb.organisation_code
left join organisation as region
    on practice.region_code = region.organisation_code
left join wnl_practice
    on practice.practice_code = wnl_practice.practice_code
