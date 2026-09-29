{{ config(materialized='table') }}

select
    unique_service_request_identifier
    , service_request_identifier
    , unique_care_contact_identifier
    , care_contact_identifier
    , care_contact_date
    , care_contact_time
    , ic_age_at_care_contact_date
    , activity_location_type_code
    , attendance_status
    , attended_or_did_not_attend_code
    , clinical_contact_duration_of_care_contact
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , dm_commissioner_derivation_reason
    , organisation_code_code_of_commissioner
    , consultation_mechanism_community_care
    , consultation_medium_used
    , administrative_category_code
    , consultation_type
    , care_contact_subject
    , site_code_of_treatment
    , group_therapy_indicator
    , care_professional_team_local_identifier
    , unique_care_professional_team_local_identifier
    , earliest_reasonable_offer_date
    , earliest_clinically_appropriate_date
    , care_contact_cancellation_date
    , care_contact_cancellation_reason
    , replacement_appointment_date_offered
    , replacement_appointment_booked_date
    , cyp201_unique_id
    , person_id
    , unique_submission_id
    , organisation_code_provider
    , organisation_identifier_code_of_provider
    , effective_from
    , reporting_period_start_date
    , reporting_period_end_date
    , file_type
    , csds_version
    , organisation_code_provider as org_id_prov
from {{ ref('stg_csds_care_contact_history') }}
qualify row_number() over (
    partition by unique_service_request_identifier, unique_care_contact_identifier
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id desc, cyp201_unique_id desc
) = 1
