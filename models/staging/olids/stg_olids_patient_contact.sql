select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    contact_type_source_concept_id,
    start_date,
    end_date,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id
from {{ ref('raw_olids_patient_contact') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
