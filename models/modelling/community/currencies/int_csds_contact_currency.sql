with activity as (
    select
        c.unique_service_request_identifier
        , c.unique_care_contact_identifier
        , c.person_id
        , b.sk_patient_id
        , c.org_id_prov
        , c.care_contact_date
        , r.ic_age_at_service_referral_received_date
        , coalesce(c.dm_icb_commissioner, r.dm_icb_commissioner) as dm_icb_commissioner
        , coalesce(c.dm_sub_icb_commissioner, r.dm_sub_icb_commissioner) as dm_sub_icb_commissioner
        , c.attendance_status
        , nullif(trim(st.team_type_code), '') as team_type_code
        , nullif(trim(r.primary_reason_for_referral_community_care), '') as primary_referral_reason
    from {{ ref('stg_csds_cyp201carecontact') }} as c
    left join {{ ref('stg_csds_cyp101referral') }} as r
        on c.unique_service_request_identifier = r.unique_service_request_identifier
    left join {{ ref('stg_csds_servicetype') }} as st
        on c.unique_service_request_identifier = st.unique_service_request_identifier
    left join {{ ref('stg_csds_bridging') }} as b
        on c.person_id = b.person_id
)

, practice_at_contact as (
    select
        a.unique_service_request_identifier
        , a.unique_care_contact_identifier
        , gp.practice_code
    from activity as a
    inner join {{ ref('stg_csds_gp_registration') }} as gp
        on a.person_id = gp.person_id
        and a.care_contact_date >= gp.registration_start_date
        and (
            gp.registration_end_date is null
            or a.care_contact_date < gp.registration_end_date
        )
    qualify row_number() over (
        partition by a.unique_service_request_identifier, a.unique_care_contact_identifier
        order by gp.registration_start_date desc nulls last, gp.practice_code
    ) = 1
)

, latest_gp_registration as (
    select
        person_id
        , practice_code
    from {{ ref('stg_csds_gp_registration') }}
    qualify row_number() over (
        partition by person_id
        order by registration_start_date desc nulls last, practice_code
    ) = 1
)

, practice_assignment as (
    select
        a.unique_service_request_identifier
        , a.unique_care_contact_identifier
        , coalesce(at_contact.practice_code, latest.practice_code) as practice_code
        , case
            when at_contact.practice_code is not null then 'at_contact'
            when latest.practice_code is not null then 'latest_known'
        end as practice_attribution
    from activity as a
    left join practice_at_contact as at_contact
        on a.unique_service_request_identifier = at_contact.unique_service_request_identifier
        and a.unique_care_contact_identifier = at_contact.unique_care_contact_identifier
    left join latest_gp_registration as latest
        on a.person_id = latest.person_id
)

, practice_context as (
    select
        practice_code
        , practice_name
        , pcn_code
        , pcn_name
        , registered_borough_name as practice_registered_borough
    from {{ ref('stg_reference_primary_care_pcn_membership_all') }}
    -- REVIEW: The lookup has no membership dates. If duplicate practice codes
    -- emerge, prefer Active practice status before the PCN code tie-breaker.
    qualify row_number() over (
        partition by practice_code
        order by iff(practice_status = 'Active', 0, 1), pcn_code
    ) = 1
)

-- national fallback for practices outside the WNL PCN lookup (out-of-area
-- registrations): name only, from the ODS practice dimension
, ods_practice as (
    select
        organisation_code as practice_code
        , organisation_name as practice_name
    from {{ ref('stg_ukhfd_all_gp_and_gdp_practices') }}
)

, residence as (
    select
        person_id
        , lsoa21_residence
        , sub_icb_of_residence
    from {{ ref('stg_csds_mpi') }}
)

, lsoa_to_lad as (
    select
        lsoa21_cd
        , lad26_nm as residence_borough
    from {{ ref('stg_reference_geo_lsoa21_sicbl26_icb26_nhser26_lad26') }}
)

-- ~5% of submitted "2021" LSOAs are retired 2011 codes; bridge them to a
-- 2021 code (any split member - LAD is stable across splits) and resolve
, lsoa11_to_lad as (
    select distinct
        b.old_lsoa_code as lsoa11_cd
        , l.residence_borough
    from {{ ref('stg_ukhfd_old_lsoa_to_new_lsoa_map') }} as b
    inner join lsoa_to_lad as l
        on b.new_lsoa_code = l.lsoa21_cd
)

, enriched as (
    select
        a.*
        , practice.practice_code
        , practice.practice_attribution
        , coalesce(context.practice_name, ods.practice_name) as practice_name
        , case
            when context.practice_code is not null then 'wnl_pcn_lookup'
            when ods.practice_code is not null then 'ods_national'
        end as practice_metadata_source
        , context.pcn_code
        , context.pcn_name
        , context.practice_registered_borough
        , residence.lsoa21_residence
        , coalesce(geography.residence_borough, geography_2011.residence_borough) as residence_borough
        , residence.sub_icb_of_residence
    from activity as a
    left join practice_assignment as practice
        on a.unique_service_request_identifier = practice.unique_service_request_identifier
        and a.unique_care_contact_identifier = practice.unique_care_contact_identifier
    left join practice_context as context
        on practice.practice_code = context.practice_code
    left join ods_practice as ods
        on practice.practice_code = ods.practice_code
    left join residence
        on a.person_id = residence.person_id
    left join lsoa_to_lad as geography
        on residence.lsoa21_residence = geography.lsoa21_cd
    left join lsoa11_to_lad as geography_2011
        on residence.lsoa21_residence = geography_2011.lsoa11_cd
)

, categorised as (
    select
        *
        , case
            when ic_age_at_service_referral_received_date is null
                or ic_age_at_service_referral_received_date < 0 then 'Adult'
            when ic_age_at_service_referral_received_date between 0 and 17 then 'CYP'
            else 'Adult'
        end as age_category
        -- lpad guards against zero-padded codes; nulls (~54% of contacts,
        -- structural across providers) costed per NHSE v1.1 guidance
        , lpad(attendance_status, 2, '0') in ('05', '06') or attendance_status is null as is_costed_attendance
    from enriched
)

, classified as (
    select
        c.*
        , tt.team_type_name
        , tt.currency_code as team_currency_code
        , tt.is_reason_split
        , rr.currency_code as reason_currency_code
    from categorised as c
    left join {{ ref('nhse_community_currency_team_types_2526') }} as tt
        on c.age_category = tt.age_category
        and c.team_type_code = tt.team_type_code
    left join {{ ref('nhse_community_currency_referral_reasons_2526') }} as rr
        on c.age_category = rr.age_category
        and c.team_type_code = rr.team_type_code
        and c.primary_referral_reason = rr.primary_referral_reason
)

select
    unique_service_request_identifier
    , unique_care_contact_identifier
    , person_id
    , sk_patient_id
    , org_id_prov
    , care_contact_date
    , ic_age_at_service_referral_received_date
    , classified.dm_icb_commissioner
    , dm_sub_icb_commissioner
    , coalesce(comm.icb_code, iff(left(classified.dm_icb_commissioner, 1) = 'Q', classified.dm_icb_commissioner, null)) as commissioner_icb_code
    , practice_code
    , practice_attribution
    , practice_name
    , practice_metadata_source
    , pcn_code
    , pcn_name
    , practice_registered_borough
    , lsoa21_residence
    , residence_borough
    , sub_icb_of_residence
    , age_category
    , attendance_status
    , is_costed_attendance
    , team_type_code
    , team_type_name
    , primary_referral_reason
    , case
        when team_currency_code is not null and is_reason_split then coalesce(reason_currency_code, team_currency_code)
        when team_currency_code is not null then team_currency_code
        else iff(age_category = 'CYP', 'CCO99Z', 'CAO99Z')
    end as currency_code
    , case
        when team_currency_code is null then 'unmapped_team'
        when is_reason_split and reason_currency_code is not null then 'reason_split'
        when is_reason_split then 'team_fallback'
        else 'team_type'
    end as currency_source
from classified
left join {{ ref('wnl_commissioner_icb_lookup') }} as comm
    on classified.dm_icb_commissioner = comm.commissioner_code
