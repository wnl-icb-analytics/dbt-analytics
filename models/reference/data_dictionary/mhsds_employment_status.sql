{# The two lists do not share codes; if they ever do, the newest definition wins. #}
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
from {{ ref('mhsds_employment_status_history') }}
where is_latest_definition
qualify row_number() over (
    partition by code
    order by
        source_effective_from_at desc nulls last,
        source_imported_at desc nulls last,
        source_unique_key desc,
        source_code_set_name = 'Employment_Status_For_Mental_Health' desc
) = 1
