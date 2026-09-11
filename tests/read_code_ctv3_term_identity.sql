select r.code
from {{ ref('read_code') }} as r
left join {{ ref('stg_ukhfd_ctv3_term') }} as t on r.code = t.code
left join {{ ref('stg_ukhfd_ctv3_concept') }} as c on r.code = c.code
where r.match_type = 'ctv3_term_code'
    and (r.coding_system <> 'ctv3' or t.code is null or c.code is not null
        or r.term is distinct from t.term or r.snomed_ct_code is not null)
