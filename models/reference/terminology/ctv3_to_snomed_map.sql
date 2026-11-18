-- Current map per code and term: the latest revision, kept only if it is active.
select
    ctv3_code,
    ctv3_term_id,
    ctv3_term_type,
    ctv3_term_type = 'P' as is_preferred_term,
    snomed_concept_id,
    snomed_description_id,
    is_assured,
    map_effective_date
from {{ ref('stg_ukhfd_ctv3_to_snomed_map') }}
qualify map_effective_date = max(map_effective_date) over (partition by ctv3_code, ctv3_term_id)
    and is_current_map
