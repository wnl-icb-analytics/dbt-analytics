{# Currently valid definitions first, then the current code set over the retired one. #}
select
    code,
    description,
    short_description,
    category,
    notes,
    valid_from_date,
    valid_to_date,
    is_currently_valid,
    source_code_set_name,
    source_imported_at,
    source_effective_from_at as definition_updated_at
from {{ ref('mhsds_referring_care_professional_type_history') }}
where is_latest_definition
qualify row_number() over (
    partition by code
    order by
        is_currently_valid desc,
        source_code_set_name = 'Referring_Care_Professional_Type_Mental_Health' desc,
        source_effective_from_at desc nulls last,
        source_imported_at desc
) = 1
