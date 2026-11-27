select super_type_id, sub_type_id
from {{ ref('raw_nhsd_snomed_sct_transitive_closure') }}
