-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_refresh_log. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    run_id,
    feed,
    started_at,
    completed_at,
    new_files_matched,
    new_exceptions,
    total_mapped_files,
    total_exceptions,
    status,
    error_message

from {{ ref('raw_sdl_wnl_meta_refresh_log') }}
