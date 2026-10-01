-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_row_count_compare. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    source_rows,
    final_rows,
    gap,
    coverage_pct

from {{ ref('raw_sdl_wnl_meta_row_count_compare') }}
