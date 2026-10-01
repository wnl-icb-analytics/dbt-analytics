select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    date_recorded,
    description,
    procedure_request_source_concept_id,
    status_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    is_confidential,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_procedure_request') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
