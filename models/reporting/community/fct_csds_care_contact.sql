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
select
    {{ dbt_utils.generate_surrogate_key(['r.unique_service_request_identifier', 'r.unique_care_contact_identifier']) }} as source_record_id
    , 'CSDS' as source_dataset
    , r.unique_service_request_identifier as referral_id
    , r.unique_care_contact_identifier as contact_id
    , r.care_contact_identifier as local_contact_id
    , r.cyp201_unique_id as source_row_id
    , r.person_id
    , b.sk_patient_id
    , r.ic_age_at_care_contact_date as age_at_contact
    , r.care_contact_date::date as care_contact_date
    , r.care_contact_time::time as care_contact_time
    , case
        when r.care_contact_date is null then null
        when r.care_contact_time is null then r.care_contact_date::date::timestamp_ntz
        else timestamp_ntz_from_parts(r.care_contact_date::date, r.care_contact_time::time)
    end as care_contact_at
    , case
        when r.care_contact_date is null then null
        when r.care_contact_time is null then 'date'
        else 'timestamp'
    end as care_contact_time_precision
    , r.attended_or_did_not_attend_code as attendance_code
    , att.description as attendance_name
    , r.attendance_status as source_attendance_status_code
    , source_attendance.description as source_attendance_status_name
    , r.consultation_mechanism_community_care as consultation_mechanism_code
    , cm.description as consultation_mechanism_name
    , r.consultation_medium_used as legacy_consultation_medium_code
    , r.activity_location_type_code
    , loc.description as activity_location_type_name
    , r.clinical_contact_duration_of_care_contact as clinical_contact_duration_minutes
    , r.site_code_of_treatment as site_code
    , site.organisation_name as site_name
    , case
        when nullif(trim(r.site_code_of_treatment), '') is null then 'missing'
        when site.organisation_code is not null then 'exact_reference_match'
        when site_organisation.organisation_code is not null then 'padded_organisation_code'
        else 'unrecognised'
    end as site_code_status
    , site_organisation.organisation_code as site_organisation_code
    , site_organisation.organisation_name as site_organisation_name
    , site_organisation.name_source as site_organisation_name_source
    , r.unique_care_professional_team_local_identifier as team_id
    , r.care_professional_team_local_identifier as local_team_id
    , r.administrative_category_code
    , r.consultation_type as consultation_type_code
    , r.care_contact_subject as care_contact_subject_code
    , r.group_therapy_indicator as group_therapy_code
    , r.care_contact_cancellation_date::date as cancellation_date
    , r.care_contact_cancellation_reason as cancellation_reason_code
    , r.earliest_reasonable_offer_date::date as earliest_reasonable_offer_date
    , r.earliest_clinically_appropriate_date::date as earliest_clinically_appropriate_date
    , r.replacement_appointment_date_offered::date as replacement_appointment_date_offered
    , r.replacement_appointment_booked_date::date as replacement_appointment_booked_date
    , submitted_referral.cyp101_unique_id as referral_source_row_id
    , submitted_referral.cyp101_unique_id is not null as is_submitted_referral_linked
    , case
        when submitted_referral.cyp101_unique_id is null
            or r.person_id is null or submitted_referral.person_id is null then null
        else r.person_id = submitted_referral.person_id
    end as is_submitted_referral_person_consistent
    , p.source_record_id is not null as is_referral_linked
    , case
        when p.source_record_id is null or r.person_id is null or p.person_id is null then null
        else r.person_id = p.person_id
    end as is_referral_person_consistent
    , r.organisation_code_provider as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , r.organisation_code_code_of_commissioner as submitted_commissioner_code
    , commissioner.organisation_name as submitted_commissioner_name
    , r.dm_icb_commissioner as source_icb_commissioner_code
    , icb.organisation_name as source_icb_commissioner_name
    , r.dm_sub_icb_commissioner as source_sub_icb_commissioner_code
    , sub_icb.organisation_name as source_sub_icb_commissioner_name
    , r.dm_commissioner_derivation_reason as source_commissioner_derivation_reason
    , r.unique_submission_id as submission_id
    , r.reporting_period_start_date::date as reporting_period_start_date
    , r.reporting_period_end_date::date as reporting_period_end_date
    , r.effective_from as source_file_received_at
    , r.csds_version
    , r.file_type
    , admin.description as administrative_category_name
    , consult.description as consultation_type_name
    , subject.description as care_contact_subject_name
    , cancel.description as cancellation_reason_name
    , medium.description as legacy_consultation_medium_name
    , therapy.description as group_therapy_name
from {{ ref('stg_csds_cyp201carecontact') }} as r
left join {{ ref('stg_csds_bridging') }} as b on r.person_id = b.person_id
left join {{ ref('stg_csds_referral_history') }} as submitted_referral
    on r.unique_submission_id = submitted_referral.unique_submission_id
    and r.unique_service_request_identifier = submitted_referral.unique_service_request_identifier
left join {{ ref('fct_csds_referral') }} as p on r.unique_service_request_identifier = p.source_record_id
left join {{ ref('attendance_status') }} as att on nullif(ltrim(trim(r.attended_or_did_not_attend_code), '0'), '') = att.code
left join {{ ref('consultation_mechanism') }} as cm on trim(r.consultation_mechanism_community_care) = cm.code
left join {{ ref('activity_location_type') }} as loc on trim(r.activity_location_type_code) = loc.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as admin
    on admin.code_set_name = 'administrative_category' and trim(r.administrative_category_code) = admin.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as consult
    on consult.code_set_name = 'consultation_type' and trim(r.consultation_type) = consult.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as subject
    on subject.code_set_name = 'care_contact_subject' and trim(r.care_contact_subject) = subject.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as cancel
    on cancel.code_set_name = 'care_contact_cancellation_reason' and trim(r.care_contact_cancellation_reason) = cancel.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as medium
    on medium.code_set_name = 'consultation_medium_used' and trim(r.consultation_medium_used) = medium.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as therapy
    on therapy.code_set_name = 'group_therapy_indicator' and trim(r.group_therapy_indicator) = therapy.code
left join {{ ref('attendance_status') }} as source_attendance
    on nullif(ltrim(trim(r.attendance_status), '0'), '') = source_attendance.code
left join {{ ref('organisation') }} as provider
    on upper(trim(r.organisation_code_provider)) = provider.organisation_code
left join {{ ref('organisation') }} as commissioner
    on upper(trim(r.organisation_code_code_of_commissioner)) = commissioner.organisation_code
left join {{ ref('organisation') }} as icb
    on upper(trim(r.dm_icb_commissioner)) = icb.organisation_code
left join {{ ref('organisation') }} as sub_icb
    on upper(trim(r.dm_sub_icb_commissioner)) = sub_icb.organisation_code
left join {{ ref('organisation') }} as site
    on upper(trim(r.site_code_of_treatment)) = site.organisation_code
-- Historical trust padding identifies an organisation, not a physical site.
left join organisation_type as site_organisation_type
    on site.organisation_code is null
    and left(upper(trim(r.site_code_of_treatment)), 3) = site_organisation_type.organisation_code
    and (
        (regexp_like(upper(trim(r.site_code_of_treatment)), 'R[A-Z0-9]{2}00')
            and site_organisation_type.organisation_primary_role = 'RO197')
        or (regexp_like(upper(trim(r.site_code_of_treatment)), 'T[A-Z0-9]{2}00')
            and site_organisation_type.organisation_primary_role = 'RO107')
        or (regexp_like(upper(trim(r.site_code_of_treatment)), '5[A-Z0-9]{2}00')
            and site_organisation_type.organisation_primary_role = 'RO179')
    )
left join {{ ref('organisation') }} as site_organisation
    on site_organisation_type.organisation_code = site_organisation.organisation_code
