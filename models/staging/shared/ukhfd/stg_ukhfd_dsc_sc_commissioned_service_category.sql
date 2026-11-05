select
    nullif(trim(commissioned_service_category_code), '') as commissioned_service_category_code
    , nullif(trim(commissioned_service_category_code_description), '') as commissioned_service_category_description
    , in_source_data = 1 as is_in_latest_source
    , is_latest = 1 as is_latest_definition
    , effective_snapshot_date as source_snapshot_date
    , effective_from as source_effective_from_at
    , effective_to as source_effective_to_at
from {{ ref('raw_ukhfd_dsc_sc_commissioned_service_category') }}
