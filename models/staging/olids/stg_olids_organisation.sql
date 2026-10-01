select
    -- Primary key
    id,

    -- Business columns
    organisation_code,
    assigning_authority_code as organisation_code_assigning_authority,
    name,
    type_description as description, -- REVIEW: nearest equivalent
    primary_location_type_source_concept_id as location_type_source_concept_id,
    postcode,
    parent_organisation_id,
    open_date,
    close_date,
    is_obsolete,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id
from {{ ref('raw_olids_organisation') }}
where coalesce(lds_is_deleted, false) = false
