{{ config(materialized='table') }}

select
    unique_service_request_identifier
    , service_request_identifier
    , local_patient_identifier_extended
    , referral_request_received_date
    , referral_request_received_time
    , primary_reason_for_referral_community_care
    , service_discharge_date
    , priority_type_code
    , ic_age_at_service_referral_received_date
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , dm_commissioner_derivation_reason
    , organisation_code_code_of_commissioner
    , source_of_referral_for_community
    , referring_organisation_code
    , referring_care_professional_staff_group_community_care
    , discharge_letter_issued_date_community_care
    , cyp101_unique_id
    , record_start_date
    , record_end_date
    , person_id
    , unique_submission_id
    , organisation_code_provider
    , organisation_identifier_code_of_provider
    , effective_from
    , reporting_period_start_date
    , reporting_period_end_date
    , file_type
    , csds_version
from {{ ref('stg_csds_referral_history') }}
qualify row_number() over (
    partition by unique_service_request_identifier
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id desc, cyp101_unique_id desc
) = 1
