select count(*) as missing_declared_read_labels
from {{ ref('fct_mhsds_presenting_complaint') }} as c
inner join {{ ref('read_code') }} as r
    on trim(c.complaint_code) = r.code
    and r.coding_system = case c.finding_scheme_code when '02' then 'read_v2' when '03' then 'ctv3' end
where r.term is not null and c.complaint_description is distinct from r.term
having missing_declared_read_labels > 0
