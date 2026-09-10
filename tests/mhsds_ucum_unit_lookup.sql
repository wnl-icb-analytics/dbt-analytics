select 'canonical_ucum_precedence' as failure_reason, count(*) as failures
from {{ ref('ucum_unit') }} as u
left join {{ ref('clinical_unit_of_measurement') }} as m
    on u.code = m.code
where m.match_type is distinct from 'ucum_code'
    or u.code is distinct from m.unit_symbol
having count(*) > 0

union all

select 'historical_code_omitted', count(*)
from (select distinct code from {{ ref('ucum_unit_history') }}) as h
left join {{ ref('ucum_unit') }} as u on h.code = u.code
where u.code is null
having count(*) > 0
