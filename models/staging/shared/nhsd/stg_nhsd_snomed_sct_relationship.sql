select id, effective_time, active, module_id, source_id, destination_id,
    relationship_group, type_id, characteristic_type_id, modifier_id
from {{ ref('raw_nhsd_snomed_sct_relationship') }}
