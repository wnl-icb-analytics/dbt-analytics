{{ config(materialized='ephemeral') }}

select
    hrg_code,
    hrg_name,
    is_in_source_data as is_listed,
    valid_from_date,
    valid_to_date,
    source_table_name,
    is_latest_revision,
    source_imported_at
from {{ ref('stg_ukhfd_pbr_all_hrgs') }}
