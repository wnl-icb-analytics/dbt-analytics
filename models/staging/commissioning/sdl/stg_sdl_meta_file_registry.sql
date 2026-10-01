-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_file_registry. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    file_id,
    batch_id,
    file_name,
    original_file_name,
    created_datetime,
    row_count,
    loader_job_name,
    last_refreshed_at

from {{ ref('raw_sdl_wnl_meta_file_registry') }}
