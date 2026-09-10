select
    r.unique_service_request_identifier as source_record_id
    , 'CSDS' as source_dataset
    , r.unique_service_request_identifier as referral_id
    , r.service_request_identifier as local_referral_id
    , r.cyp101_unique_id as source_row_id
    , r.person_id
    , b.sk_patient_id
    , r.ic_age_at_service_referral_received_date as age_at_referral
    , r.referral_request_received_date::date as referral_received_date
    , r.referral_request_received_time::time as referral_received_time
    , case
        when r.referral_request_received_date is null then null
        when r.referral_request_received_time is null then r.referral_request_received_date::date::timestamp_ntz
        else timestamp_ntz_from_parts(r.referral_request_received_date::date, r.referral_request_received_time::time)
    end as referral_received_at
    , case
        when r.referral_request_received_date is null then null
        when r.referral_request_received_time is null then 'date'
        else 'timestamp'
    end as referral_received_time_precision
    , r.service_discharge_date::date as service_discharge_date
    , r.source_of_referral_for_community as source_of_referral_code
    , s.description as source_of_referral_name
    , r.primary_reason_for_referral_community_care as primary_reason_for_referral_code
    , reason.description as primary_reason_for_referral_name
    , r.priority_type_code
    , priority.description as priority_type_name
    , staff.description as referring_professional_staff_group_name
    , r.referring_organisation_code
    , referring.organisation_name as referring_organisation_name
    , r.referring_care_professional_staff_group_community_care as referring_professional_staff_group_code
    , r.discharge_letter_issued_date_community_care::date as discharge_letter_issued_date
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
from {{ ref('stg_csds_cyp101referral') }} as r
left join {{ ref('stg_csds_bridging') }} as b on r.person_id = b.person_id
left join {{ ref('csds_source_of_referral') }} as s on trim(r.source_of_referral_for_community) = s.code
left join {{ ref('csds_referral_code_lookup') }} as reason
    on reason.code_set_name = 'reason_for_referral' and trim(r.primary_reason_for_referral_community_care) = reason.code
left join {{ ref('csds_referral_code_lookup') }} as priority
    on priority.code_set_name = 'priority_type' and trim(r.priority_type_code) = priority.code
left join {{ ref('csds_referral_code_lookup') }} as staff
    on staff.code_set_name = 'referring_staff_group' and trim(r.referring_care_professional_staff_group_community_care) = staff.code
left join {{ ref('organisation') }} as provider
    on upper(trim(r.organisation_code_provider)) = provider.organisation_code
left join {{ ref('organisation') }} as commissioner
    on upper(trim(r.organisation_code_code_of_commissioner)) = commissioner.organisation_code
left join {{ ref('organisation') }} as icb
    on upper(trim(r.dm_icb_commissioner)) = icb.organisation_code
left join {{ ref('organisation') }} as sub_icb
    on upper(trim(r.dm_sub_icb_commissioner)) = sub_icb.organisation_code
left join {{ ref('organisation') }} as referring
    on upper(trim(r.referring_organisation_code)) = referring.organisation_code
