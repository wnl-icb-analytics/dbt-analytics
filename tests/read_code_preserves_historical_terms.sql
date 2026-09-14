-- Every historical full term code stays available; term-specific labels never inherit a concept mapping.
select coalesce(h.code, r.code) as code
from {{ ref('stg_ukhfd_read_v2_term') }} as h
full outer join (
    select * from {{ ref('read_code') }} where coding_system = 'read_v2'
) as r on h.code = r.code
where (h.code is not null and r.code is null)
    or (r.definition_source = 'UKHFD Read v2 term history' and (
        h.code is null or length(r.code) <> 7
        or r.term is distinct from h.term
        or r.match_type is distinct from 'read_v2_term_code'
        or r.dictionary_read_code is not null or r.snomed_ct_code is not null
    ))
