select snomed_concept_id, snomed_term, opcs4_code, opcs4_term, opcs_version,
    map_block, map_group, map_priority, map_rule, map_advice, effective_time, release_id
from {{ ref('raw_reference_snomed_to_opcs4_latest') }}
