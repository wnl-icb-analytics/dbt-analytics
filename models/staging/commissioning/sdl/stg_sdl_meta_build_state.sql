-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_build_state. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    max_file_id_loaded,
    last_built_at,
    last_bulk_at,
    needs_rebuild,
    last_error

from {{ ref('raw_sdl_wnl_meta_build_state') }}
