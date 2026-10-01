select distinct
    history.code_set_name
    , history.code
from {{ ref('mhsds_inpatient_code_lookup_history') }} as history
left join {{ ref('mhsds_inpatient_code_lookup') }} as lookup
    on history.code_set_name = lookup.code_set_name
    and history.code = lookup.code
where history.is_latest_definition
    and lookup.code is null
