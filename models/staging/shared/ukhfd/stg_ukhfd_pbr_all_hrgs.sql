select
    upper(trim(hrg_code)) as hrg_code,
    nullif(trim(hrg_name), '') as hrg_name,
    nullif(trim(source_table), '') as source_table_name,
    in_source_data = 1 as is_in_source_data,
    -- The earliest revision has no effective date; UKHFD created it on 1 April 2010.
    cast(coalesce(effective_from, created_date) as date) as valid_from_date,
    cast(effective_to as date) as valid_to_date,
    is_latest = 1 as is_latest_revision,
    import_date as source_imported_at
from {{ ref('raw_ukhfd_pbr_all_hrgs') }}
