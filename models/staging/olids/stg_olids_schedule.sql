{{ config(materialized='table') }}

-- Materialised as a table because the qualify row_number() dedup below would
-- otherwise re-run on every downstream query (default for staging is view).
select
    -- Primary key
    id,

    -- Business columns
    location_id,
    location_name,
    practitioner_id,
    start_datetime::date as start_date, -- REVIEW: narrows the upstream timestamp to the old date type
    end_datetime::date as end_date, -- REVIEW: narrows the upstream timestamp to the old date type
    type,
    name,
    is_private,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_schedule') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_transform_datetime desc
) = 1
