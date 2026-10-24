with candidates as (
    select 'read_v2' as coding_system, read_code as code,
        snomed_concept_id as snomed_code, 'National Read v2 map' as snomed_mapping_method,
        is_assured, 1 as priority
    from {{ ref('read_v2_to_snomed_map') }}
    where is_preferred_term

    union all

    select 'ctv3', ctv3_code, snomed_concept_id, 'National CTV3 map', is_assured, 1
    from {{ ref('ctv3_to_snomed_map') }}
    where is_preferred_term

    union all

    select coding_system, code, snomed_ct_code, 'Legacy Read dictionary', null::boolean, 2
    from {{ ref('read_code') }}
    where snomed_ct_code is not null
)
select
    c.coding_system,
    c.code,
    s.snomed_code,
    s.preferred_term as snomed_code_name,
    c.snomed_mapping_method,
    c.is_assured
from candidates as c
inner join {{ ref('snomed_concept') }} as s on c.snomed_code = s.snomed_code
qualify row_number() over (partition by c.coding_system, c.code order by c.priority) = 1
