select
    r.id as source_record_id,
    r.observation_id,
    r.referral_snomed_code,
    r.referral_snomed_name,
    r.provider_organisation_id,
    provider.organisation_code as provider_organisation_code,
    nullif(trim(provider.name), '') as provider_organisation_name,
    r.person_id,
    p.sk_patient_id,
    r.patient_id,
    r.encounter_id,
    r.practitioner_id,
    r.unique_booking_reference_number,
    r.clinical_effective_date,
    r.date_recorded,
    r.mode,
    r.is_outgoing_referral,
    r.is_review,
    r.requester_organisation_id as referring_organisation_id,
    referring.organisation_code as referring_organisation_code,
    referring.organisation_code_assigning_authority as referring_organisation_code_authority,
    nullif(trim(referring.name), '') as referring_organisation_name,
    r.recipient_organisation_id as receiving_organisation_id,
    receiving.organisation_code as receiving_organisation_code,
    receiving.organisation_code_assigning_authority as receiving_organisation_code_authority,
    nullif(trim(receiving.name), '') as receiving_organisation_name,
    r.publisher_organisation_id,
    publisher.organisation_code as publisher_organisation_code,
    publisher.organisation_code_assigning_authority as publisher_organisation_code_authority,
    nullif(trim(publisher.name), '') as publisher_organisation_name,
    r.publisher_organisation_code as source_publisher_organisation_code,
    r.referral_request_source_concept_id,
    r.source_code as referral_code,
    r.source_display as referral_name,
    r.source_system as referral_coding_system,
    r.mapped_concept_code,
    r.mapped_concept_display,
    r.target_system as mapped_concept_coding_system,
    r.referral_request_priority_source_concept_id,
    r.referral_request_priority_source_code as priority_code,
    r.referral_request_priority_source_display as priority_name,
    r.referral_request_type_source_concept_id,
    r.referral_request_type_source_code as referral_type_code,
    r.referral_request_type_source_display as referral_type_name,
    r.clinical_effective_date_precision_source_concept_id,
    r.date_precision_source_code as clinical_date_precision_code,
    r.date_precision_source_display as clinical_date_precision_name,
    r.lds_transform_datetime as source_transform_datetime,
    r.lds_source_record_id
from {{ ref('stg_olids_referral_request') }} as r
left join {{ ref('dim_person_pseudo') }} as p
    on r.person_id = p.person_id
left join {{ ref('stg_olids_organisation') }} as referring
    on r.requester_organisation_id = referring.id
left join {{ ref('stg_olids_organisation') }} as receiving
    on r.recipient_organisation_id = receiving.id
left join {{ ref('stg_olids_organisation') }} as publisher
    on r.publisher_organisation_id = publisher.id
left join {{ ref('stg_olids_organisation') }} as provider
    on r.provider_organisation_id = provider.id
