-- Aggregate-only validation. No referral, person or booking-reference values are returned.
with source_reconciliation as (
    select
        count_if(r.id is not null) as source_rows,
        count_if(f.source_record_id is not null) as fact_rows,
        count_if(r.id is not null and f.source_record_id is null) as missing_referrals,
        count_if(r.id is null and f.source_record_id is not null) as extra_referrals,
        count_if(r.id is not null and f.source_record_id is not null and (
            r.observation_id is distinct from f.observation_id
            or r.referral_snomed_code is distinct from f.referral_snomed_code
            or r.referral_snomed_name is distinct from f.referral_snomed_name
            or r.provider_organisation_id is distinct from f.provider_organisation_id
            or r.person_id is distinct from f.person_id
            or r.patient_id is distinct from f.patient_id
            or r.encounter_id is distinct from f.encounter_id
            or r.practitioner_id is distinct from f.practitioner_id
            or r.unique_booking_reference_number is distinct from f.unique_booking_reference_number
            or r.clinical_effective_date is distinct from f.clinical_effective_date
            or r.date_recorded is distinct from f.date_recorded
            or r.mode is distinct from f.mode
            or r.is_outgoing_referral is distinct from f.is_outgoing_referral
            or r.is_review is distinct from f.is_review
            or r.requester_organisation_id is distinct from f.referring_organisation_id
            or r.recipient_organisation_id is distinct from f.receiving_organisation_id
            or r.publisher_organisation_id is distinct from f.publisher_organisation_id
            or r.publisher_organisation_code is distinct from f.source_publisher_organisation_code
            or r.referral_request_source_concept_id is distinct from f.referral_request_source_concept_id
            or r.mapped_concept_code is distinct from f.mapped_concept_code
            or r.mapped_concept_display is distinct from f.mapped_concept_display
            or r.referral_request_priority_source_concept_id is distinct from f.referral_request_priority_source_concept_id
            or r.referral_request_type_source_concept_id is distinct from f.referral_request_type_source_concept_id
            or r.clinical_effective_date_precision_source_concept_id is distinct from f.clinical_effective_date_precision_source_concept_id
            or r.source_code is distinct from f.referral_code
            or r.source_display is distinct from f.referral_name
            or r.source_system is distinct from f.referral_coding_system
            or r.target_system is distinct from f.mapped_concept_coding_system
            or r.referral_request_priority_source_code is distinct from f.priority_code
            or r.referral_request_priority_source_display is distinct from f.priority_name
            or r.referral_request_type_source_code is distinct from f.referral_type_code
            or r.referral_request_type_source_display is distinct from f.referral_type_name
            or r.date_precision_source_code is distinct from f.clinical_date_precision_code
            or r.date_precision_source_display is distinct from f.clinical_date_precision_name
            or r.lds_transform_datetime is distinct from f.source_transform_datetime
            or r.lds_source_record_id is distinct from f.lds_source_record_id
        )) as changed_source_rows
    from {{ ref('stg_olids_referral_request') }} as r
    full outer join {{ ref('fct_gp_referral_request') }} as f
        on r.id = f.source_record_id
),
coverage as (
    select
        count(*) as rows_after_encounter_join,
        count_if(f.sk_patient_id is not null) as patient_key_rows,
        count_if(f.encounter_id is not null) as recorded_encounter_rows,
        count_if(e.id is not null) as matched_encounter_rows,
        count_if(e.id is not null and e.person_id is distinct from f.person_id) as encounter_person_disagreements,
        count_if(nullif(trim(f.unique_booking_reference_number), '') is not null) as booking_reference_rows,
        count_if(f.clinical_effective_date is not null) as clinical_date_rows,
        count_if(f.referral_request_source_concept_id is not null) as referral_concept_rows,
        count_if(f.referral_name is not null) as labelled_referral_rows,
        count_if(f.referral_request_priority_source_concept_id is not null) as priority_concept_rows,
        count_if(f.priority_name is not null) as labelled_priority_rows,
        count_if(f.referral_request_type_source_concept_id is not null) as type_concept_rows,
        count_if(f.referral_type_name is not null) as labelled_type_rows,
        count_if(f.clinical_effective_date_precision_source_concept_id is not null) as date_precision_concept_rows,
        count_if(f.clinical_date_precision_name is not null) as labelled_date_precision_rows,
        count_if(f.referring_organisation_id is not null) as referring_organisation_rows,
        count_if(f.referring_organisation_name is not null) as labelled_referring_organisation_rows,
        count_if(f.receiving_organisation_id is not null) as receiving_organisation_rows,
        count_if(f.receiving_organisation_name is not null) as labelled_receiving_organisation_rows,
        count_if(f.publisher_organisation_id is not null) as publisher_organisation_rows,
        count_if(f.publisher_organisation_name is not null) as labelled_publisher_organisation_rows
    from {{ ref('fct_gp_referral_request') }} as f
    left join {{ ref('stg_olids_encounter') }} as e
        on f.encounter_id = e.id
)
select *
from source_reconciliation
cross join coverage
