-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_schema_versions. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading.

select
    feed,
    version_id,
    file_count,
    first_seen,
    last_seen,
    raw_mapping,
    column_mapping

from {{ ref('raw_sdl_wnl_meta_schema_versions') }}
