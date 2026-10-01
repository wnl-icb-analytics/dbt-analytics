select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    practitioner_id,
    encounter_id,
    clinical_status,
    verification_status,
    category,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    date_precision_code,
    date_precision_display,
    date_precision_source_code,
    date_precision_source_display,
    is_review,
    medication_name,
    multi_lex_action,
    allergy_intolerance_source_concept_id,
    source_code,
    source_display,
    source_system,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    target_system,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    date_recorded,
    is_confidential,
    person_id,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_allergy_intolerance') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
