with organisation_type as (
    select
        upper(trim(organisation_code)) as organisation_code
        , organisation_primary_role
    from {{ ref('stg_dictionary_dbo_organisation') }}
    where nullif(trim(organisation_code), '') is not null
    qualify row_number() over (
        partition by upper(trim(organisation_code))
        order by coalesce(last_updated, first_created) desc nulls last, sk_organisation_id desc
    ) = 1
)
-- A successful reference or parent match must remain visible in the reporting fact.
select 'referral' as source_entity, f.source_record_id
from {{ ref('fct_csds_referral') }} as f
left join {{ ref('organisation') }} as provider
    on upper(trim(f.provider_organisation_code)) = provider.organisation_code
where f.provider_organisation_name is distinct from provider.organisation_name
union all
select 'contact', f.source_record_id
from {{ ref('fct_csds_care_contact') }} as f
left join {{ ref('organisation') }} as provider
    on upper(trim(f.provider_organisation_code)) = provider.organisation_code
left join {{ ref('organisation') }} as site
    on upper(trim(f.site_code)) = site.organisation_code
left join organisation_type as site_organisation_type
    on site.organisation_code is null
    and left(upper(trim(f.site_code)), 3) = site_organisation_type.organisation_code
    and (
        (regexp_like(upper(trim(f.site_code)), 'R[A-Z0-9]{2}00')
            and site_organisation_type.organisation_primary_role = 'RO197')
        or (regexp_like(upper(trim(f.site_code)), 'T[A-Z0-9]{2}00')
            and site_organisation_type.organisation_primary_role = 'RO107')
        or (regexp_like(upper(trim(f.site_code)), '5[A-Z0-9]{2}00')
            and site_organisation_type.organisation_primary_role = 'RO179')
    )
left join {{ ref('organisation') }} as site_organisation
    on site_organisation_type.organisation_code = site_organisation.organisation_code
left join {{ ref('attendance_status') }} as attendance
    on nullif(ltrim(trim(f.source_attendance_status_code), '0'), '') = attendance.code
left join {{ ref('stg_csds_referral_history') }} as submitted_referral
    on f.submission_id = submitted_referral.unique_submission_id
    and f.referral_id = submitted_referral.unique_service_request_identifier
left join {{ ref('fct_csds_referral') }} as referral
    on f.referral_id = referral.source_record_id
where f.provider_organisation_name is distinct from provider.organisation_name
    or f.site_name is distinct from site.organisation_name
    or f.source_attendance_status_name is distinct from attendance.description
    or f.is_referral_linked is distinct from (referral.source_record_id is not null)
    or f.is_referral_person_consistent is distinct from case
        when referral.source_record_id is null or f.person_id is null or referral.person_id is null then null
        else f.person_id = referral.person_id
    end
    or f.referral_source_row_id is distinct from submitted_referral.cyp101_unique_id
    or f.is_submitted_referral_linked is distinct from (submitted_referral.cyp101_unique_id is not null)
    or f.is_submitted_referral_person_consistent is distinct from case
        when submitted_referral.cyp101_unique_id is null or f.person_id is null
            or submitted_referral.person_id is null then null
        else f.person_id = submitted_referral.person_id
    end
    or f.site_code_status is distinct from case
        when nullif(trim(f.site_code), '') is null then 'missing'
        when site.organisation_code is not null then 'exact_reference_match'
        when site_organisation.organisation_code is not null then 'padded_organisation_code'
        else 'unrecognised'
    end
    or f.site_organisation_code is distinct from site_organisation.organisation_code
    or f.site_organisation_name is distinct from site_organisation.organisation_name
    or f.site_organisation_name_source is distinct from site_organisation.name_source
