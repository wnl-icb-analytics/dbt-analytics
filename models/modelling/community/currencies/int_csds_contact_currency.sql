with activity as (
    select
        c.unique_service_request_identifier
        , c.unique_care_contact_identifier
        , c.person_id
        , b.sk_patient_id
        , c.organisation_code_provider as org_id_prov
        , c.care_contact_date
        , r.ic_age_at_service_referral_received_date
        , coalesce(c.dm_icb_commissioner, r.dm_icb_commissioner) as dm_icb_commissioner
        , coalesce(c.dm_sub_icb_commissioner, r.dm_sub_icb_commissioner) as dm_sub_icb_commissioner
        , c.attendance_status
        , {{ csds_attendance_code('c.attended_or_did_not_attend_code', 'c.attendance_status') }} as attendance_code
        , nullif(trim(st.team_type_code), '') as team_type_code
        , nullif(trim(r.primary_reason_for_referral_community_care), '') as primary_referral_reason
    from {{ ref('stg_csds_care_contact') }} as c
    left join {{ ref('stg_csds_referral') }} as r
        on c.unique_service_request_identifier = r.unique_service_request_identifier
    left join {{ ref('int_csds_currency_referral_service_type') }} as st
        on c.unique_service_request_identifier = st.unique_service_request_identifier
    left join {{ ref('stg_csds_bridging') }} as b
        on c.person_id = b.person_id
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

-- one residence per person: the newest period record across providers
, residence as (
    select
        person_id
        , residence_lsoa_2021_code as lsoa21_residence
        , residence_local_authority_name as residence_borough
        , residence_sub_icb_code as sub_icb_of_residence
    from {{ ref('dim_csds_person_provider_period') }}
    qualify row_number() over (
        partition by person_id
        order by reporting_period_end_date desc nulls last, source_file_received_at desc nulls last,
            submission_id::number desc, source_row_id::number desc
    ) = 1
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
        , residence.residence_borough
        , residence.sub_icb_of_residence
    from activity as a
    left join {{ ref('int_csds_care_contact_context') }} as practice
        on a.unique_service_request_identifier = practice.unique_service_request_identifier
        and a.unique_care_contact_identifier = practice.unique_care_contact_identifier
    left join practice_context as context
        on practice.practice_code = context.practice_code
    left join ods_practice as ods
        on practice.practice_code = ods.practice_code
    left join residence
        on a.person_id = residence.person_id
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
        -- NHSE v1.1 costs attended contacts and those with no attendance code.
        -- Both attendance fields are read: the newer status is empty before v1.6.
        , attendance_code in ('5', '6') or attendance_code is null as is_costed_attendance
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
