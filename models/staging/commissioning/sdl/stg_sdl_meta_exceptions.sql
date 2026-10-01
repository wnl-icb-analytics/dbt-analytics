-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_exceptions. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    file_id,
    batch_id,
    source_column_headers,
    detected_at,
    resolved

from {{ ref('raw_sdl_wnl_meta_exceptions') }}
