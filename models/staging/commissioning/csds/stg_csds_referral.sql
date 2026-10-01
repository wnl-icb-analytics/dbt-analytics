{{ config(materialized='table') }}

-- Rank narrow key rows, then join the wide history row back once.
with winner as (
    select
        unique_service_request_identifier
        , reporting_period_end_date
        , effective_from
        , unique_submission_id
        , cyp101_unique_id
    from {{ ref('stg_csds_referral_history') }}
    qualify row_number() over (
        partition by unique_service_request_identifier
        order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
            unique_submission_id::number desc, cyp101_unique_id::number desc
    ) = 1
)

select
    h.unique_service_request_identifier
    , h.service_request_identifier
    , h.local_patient_identifier_extended
    , h.referral_request_received_date
    , h.referral_request_received_time
    , h.primary_reason_for_referral_community_care
    , h.service_discharge_date
    , h.priority_type_code
    , h.ic_age_at_service_referral_received_date
    , h.dm_icb_commissioner
    , h.dm_sub_icb_commissioner
    , h.dm_commissioner_derivation_reason
    , h.organisation_code_code_of_commissioner
    , h.source_of_referral_for_community
    , h.referring_organisation_code
    , h.referring_care_professional_staff_group_community_care
    , h.discharge_letter_issued_date_community_care
    , h.cyp101_unique_id
    , h.record_start_date
    , h.record_end_date
    , h.person_id
    , h.unique_submission_id
    , h.organisation_code_provider
    , h.organisation_identifier_code_of_provider
    , h.effective_from
    , h.reporting_period_start_date
    , h.reporting_period_end_date
    , h.file_type
    , h.csds_version
from {{ ref('stg_csds_referral_history') }} as h
inner join winner as w
    on h.unique_submission_id = w.unique_submission_id
    and h.unique_service_request_identifier = w.unique_service_request_identifier
