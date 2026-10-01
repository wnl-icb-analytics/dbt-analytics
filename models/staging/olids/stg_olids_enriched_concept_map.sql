select
    source_concept_id,
    source_system,
    source_code,
    source_display,
    target_concept_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    equivalence,
    equivalence_rank
from {{ ref('raw_olids_enriched_concept_map') }}
