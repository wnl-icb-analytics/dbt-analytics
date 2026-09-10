-- Keep each historical code. Source presence does not establish clinical validity.
select *
from {{ ref('ucum_unit_history') }}
qualify row_number() over (
    partition by code
    order by source_effective_from_at desc nulls last,
        source_imported_at desc nulls last, source_created_at desc nulls last,
        description, synonym
) = 1
