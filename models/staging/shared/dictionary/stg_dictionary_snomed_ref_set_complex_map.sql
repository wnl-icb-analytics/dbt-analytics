select
    id
    , active
    , ref_set_id
    , referenced_component_id
    , map_group
    , map_priority
    , map_rule
    , map_advice
    , map_target
from {{ ref('raw_dictionary_snomed_ref_set_complex_map') }}
