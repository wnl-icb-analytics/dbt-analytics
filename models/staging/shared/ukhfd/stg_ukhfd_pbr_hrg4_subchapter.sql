select
    upper(trim(hrg4_sub_chapter)) as hrg_subchapter_code,
    upper(trim(hrg_chapter)) as hrg_chapter_code,
    nullif(trim(hrg4_sub_chapter_description), '') as hrg_subchapter,
    cast(effective_from as date) as valid_from_date,
    cast(effective_to as date) as valid_to_date,
    nullif(trim(file_name), '') as source_file_name,
    import_date as source_imported_at
from {{ ref('raw_ukhfd_pbr_hrg4_subchapter') }}
