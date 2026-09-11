select
    m.snomed_concept_id::varchar as snomed_code,
    coalesce(s.preferred_term, m.snomed_term) as snomed_description,
    m.opcs4_code as opcs4_code,
    coalesce(c.description, m.opcs4_term) as opcs4_description,
    m.opcs_version,
    m.map_block,
    m.map_group,
    m.map_priority,
    m.map_rule,
    m.map_advice,
    m.effective_time as effective_date,
    m.release_id
from {{ ref('stg_reference_snomed_to_opcs4_latest') }} as m
left join {{ ref('snomed_concept') }} as s on m.snomed_concept_id::varchar = s.snomed_code
left join {{ ref('opcs4_code') }} as c on m.opcs4_code = c.code
