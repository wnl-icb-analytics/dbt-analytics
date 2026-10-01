select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    address_type_source_concept_id,
    postcode as postcode_hash, -- feed pseudonymises: POSTCODE carries the hash
    is_home_address,
    start_date,
    end_date,
    person_id,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_patient_address') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
