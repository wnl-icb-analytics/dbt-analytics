{{ config(materialized='table') }}

select
    r.unique_service_request_identifier
    , r.service_request_identifier
    , r.care_professional_team_local_identifier
    , r.unique_care_professional_team_local_identifier
    , r.service_or_team_type_referred_to_community_care
    , r.referral_closure_date
    , r.referral_rejection_date
    , r.referral_closure_reason
    , r.referral_rejection_reason
    , r.cyp102_unique_id
    , r.person_id
    , r.unique_submission_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.effective_from
    , r.record_start_date
    , r.record_end_date
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp102servicetypereferredto') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
