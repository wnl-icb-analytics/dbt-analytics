select
    sk_hrgid,
    hrg_code,
    hrg_description,
    hrg_chapter_key,
    hrg_chapter,
    hrg_subchapter_key,
    hrg_subchapter,
    hrg_version,
    date_created,
    date_updated
from {{ ref('raw_dictionary_dbo_hrg') }}