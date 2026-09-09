select r.coding_system, r.code, r.dictionary_read_code, r.match_type
from {{ ref('read_code') }} as r
left join {{ ref('stg_dictionary_dbo_readcodes') }} as d
    on r.dictionary_read_code = d.read_code
where r.definition_source = 'Dictionary.dbo.ReadCodes'
    and (r.term is distinct from d.term
    or (r.coding_system = 'read_v2' and d.is_read_v2 is distinct from true)
    or (r.coding_system = 'ctv3' and d.is_ctv3 is distinct from true)
    or (r.match_type = 'dictionary_alternative' and r.code is distinct from d.read_code_alt)
    or r.snomed_ct_code is distinct from iff(try_to_number(d.snomed_ct_code) > 0, d.snomed_ct_code::varchar, null))
