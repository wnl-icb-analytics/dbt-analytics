select
    -- Primary key
    id,

    -- Business columns
    name,
    type_description,
    is_primary_location,
    house_name,
    house_number,
    house_name_flat_number,
    street,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    postcode,
    managing_organisation_id,
    open_date,
    close_date,
    is_obsolete,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    location_type_source_concept_id
from {{ ref('raw_olids_location') }}
where coalesce(lds_is_deleted, false) = false
