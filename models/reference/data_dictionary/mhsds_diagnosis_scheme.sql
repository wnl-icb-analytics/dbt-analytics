select code, description
from {{ ref('nhs_dd_diagnosis_scheme') }}

union all

select s.code, s.description
from {{ ref('mhsds_diagnosis_scheme_supplement') }} as s
where not exists (
    select 1 from {{ ref('nhs_dd_diagnosis_scheme') }} as d
    where d.code = s.code
)
