select id, effective_time, active, module_id, concept_id, language_code,
    type_id, term, case_significance_id, description_type
from {{ ref('raw_nhsd_snomed_sct_description') }}
