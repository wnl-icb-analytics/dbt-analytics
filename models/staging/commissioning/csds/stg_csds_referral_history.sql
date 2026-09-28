{{ config(materialized='table') }}

select
    r.unique_service_request_identifier
    , r.service_request_identifier
    , r.local_patient_identifier_extended
    , r.referral_request_received_date
    , r.referral_request_received_time
    , r.primary_reason_for_referral_community_care
    , r.service_discharge_date
    , r.priority_type_code
    , r.ic_age_at_service_referral_received_date
    , r.dm_icb_commissioner
    , r.dm_sub_icb_commissioner
    , r.dm_commissioner_derivation_reason
    , r.organisation_code_code_of_commissioner
    , r.source_of_referral_for_community
    , r.referring_organisation_code
    , r.referring_care_professional_staff_group_community_care
    , r.discharge_letter_issued_date_community_care
    , r.cyp101_unique_id
    , r.record_start_date
    , r.record_end_date
    , r.person_id
    , r.unique_submission_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp101referral') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
