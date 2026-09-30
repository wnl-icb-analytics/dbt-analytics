{{ config(materialized='table') }}

-- The legacy waiting_time_measurement_type alias agrees with the community-care
-- field wherever both are populated, so only the community-care field is kept.
select
    r.cyp104_unique_id
    , r.unique_service_request_identifier
    , r.service_request_identifier
    , r.person_id
    , r.unique_csds_id_patient
    , r.booking_reference_pseudo
    , r.patient_pathway_id_pseudo
    , r.organisation_code_patient_pathway_identifier_issuer
    , r.waiting_time_measurement_type_community_care
    , r.referral_to_treatment_period_start_date
    , r.referral_to_treatment_period_start_time
    , r.referral_to_treatment_period_end_date
    , r.referral_to_treatment_period_end_time
    , r.referral_to_treatment_period_status
    , r.derived_waiting_time
    , r.derived_waiting_time_night
    , r.response_standard_met
    , r.ic_age_at_referral_to_treatment_start_date
    , r.ic_age_at_referral_to_treatment_end_date
    , r.record_number
    , r.record_start_date
    , r.record_end_date
    , r.unique_submission_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp104rtt') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
