-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_null_rate_profile. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    table_name,
    column_name,
    ordinal_position,
    total_rows,
    non_null_rows,
    null_rows,
    null_rate,
    is_all_null,
    computed_at

from {{ ref('raw_sdl_wnl_meta_null_rate_profile') }}
