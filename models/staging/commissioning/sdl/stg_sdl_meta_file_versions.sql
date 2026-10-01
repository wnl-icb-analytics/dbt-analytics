-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_file_versions. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    file_id,
    batch_id,
    version_id,
    match_method

from {{ ref('raw_sdl_wnl_meta_file_versions') }}
