select
    -- Primary key
    id,

    -- Business columns
    provider_organisation_id,
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    observation_id,
    allergy_intolerance_id,
    diagnostic_order_id,
    referral_request_id,
    authorisation_type_source_concept_id,
    authorisation_type_source_code,
    authorisation_type_source_display,
    authorisation_type_code,
    authorisation_type_display,
    clinical_effective_date_precision_source_concept_id,
    date_precision_code,
    date_precision_display,
    date_precision_source_code,
    date_precision_source_display,
    medication_statement_source_concept_id,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    source_code,
    source_display,
    source_system,
    target_system,
    clinical_effective_date,
    cancellation_date,
    dose,
    quantity_value_description,
    quantity_value,
    quantity_unit,
    medication_name,
    bnf_reference,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    issue_method,
    date_recorded,
    is_active,
    is_confidential,
    expiry_date,
    publisher_organisation_code,
    lds_transform_datetime,

    -- BNF classification (pre-computed upstream in dbt-olids)
    bnf_chapter,
    bnf_section,
    bnf_code,
    bnf_name,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_medication_statement') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
