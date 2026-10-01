{{ config(materialized='table') }}

select
    r.unique_service_request_identifier
    , r.service_request_identifier
    , r.unique_care_contact_identifier
    , r.care_contact_identifier
    , r.care_contact_date
    , r.care_contact_time
    , r.ic_age_at_care_contact_date
    , r.activity_location_type_code
    , r.attendance_status
    , r.attended_or_did_not_attend_code
    , r.clinical_contact_duration_of_care_contact
    , r.dm_icb_commissioner
    , r.dm_sub_icb_commissioner
    , r.dm_commissioner_derivation_reason
    , r.organisation_code_code_of_commissioner
    , r.consultation_mechanism_community_care
    , r.consultation_medium_used
    , r.administrative_category_code
    , r.consultation_type
    , r.care_contact_subject
    , r.site_code_of_treatment
    , r.group_therapy_indicator
    , r.care_professional_team_local_identifier
    , r.unique_care_professional_team_local_identifier
    , r.earliest_reasonable_offer_date
    , r.earliest_clinically_appropriate_date
    , r.care_contact_cancellation_date
    , r.care_contact_cancellation_reason
    , r.replacement_appointment_date_offered
    , r.replacement_appointment_booked_date
    , r.cyp201_unique_id
    , r.person_id
    , r.unique_submission_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp201carecontact') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
