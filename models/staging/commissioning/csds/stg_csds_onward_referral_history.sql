{{ config(materialized='table') }}

-- The receiving-organisation, provider and reason aliases agree wherever populated,
-- so one of each is kept.
select
    r.cyp105_unique_id
    , r.unique_service_request_identifier
    , r.service_request_identifier
    , r.person_id
    , r.onward_referral_date::date as onward_referral_date
    , nullif(upper(trim(r.onward_referral_reason_community_care)), '') as onward_referral_reason
    , nullif(upper(trim(r.organisation_identifier_receiving)), '') as organisation_identifier_receiving
    , r.record_number
    , r.record_start_date
    , r.record_end_date
    , r.unique_submission_id
    , r.organisation_code_provider
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp105onwardreferral') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
