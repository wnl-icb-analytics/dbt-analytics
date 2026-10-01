select
    bnf_chapter_code,
    chapter_name,
    is_excluded
from {{ ref('bnf_polypharmacy_exclusions') }}
