select id, effective_time, active, module_id, definition_status_id
from {{ ref('raw_nhsd_snomed_sct_concept') }}
