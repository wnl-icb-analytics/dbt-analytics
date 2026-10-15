select
    commissioned_service_category_code as code
    , commissioned_service_category_description as description
    -- Drops the trailing lower-case usage note, e.g. "(funded nationally)"; keeps "(NOT LISTED)"
    , trim(regexp_replace(commissioned_service_category_description, '\\s*\\([a-z].*\\)\\s*$', '')) as short_description
    , is_in_latest_source as is_currently_valid
    , source_snapshot_date
from {{ ref('stg_ukhfd_dsc_sc_commissioned_service_category') }}
where is_latest_definition
