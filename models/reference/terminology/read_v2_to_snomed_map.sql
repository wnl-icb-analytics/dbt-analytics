-- Current map per code and term: the latest revision, kept only if it is active.
select
    read_code,
    term_code,
    term_code = '00' as is_preferred_term,
    snomed_concept_id,
    snomed_description_id,
    is_assured,
    map_effective_date
from {{ ref('stg_ukhfd_read_v2_to_snomed_map') }}
qualify map_effective_date = max(map_effective_date) over (partition by read_code, term_code)
    and is_current_map
