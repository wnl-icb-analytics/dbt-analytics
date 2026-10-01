-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_unmapped_files. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    file_id,
    batch_id,
    header_id,
    profile_code,
    original_file_name,
    row_count,
    logged_at,
    loader_job_name,
    has_headers,
    exception_detected_at,
    reason

from {{ ref('raw_sdl_wnl_meta_unmapped_files') }}
