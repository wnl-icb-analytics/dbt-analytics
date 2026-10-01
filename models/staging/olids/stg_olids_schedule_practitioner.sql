select
    -- Primary key
    id,

    -- Business columns
    schedule_id,
    practitioner_id,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_schedule_practitioner') }}
where coalesce(lds_is_deleted, false) = false
