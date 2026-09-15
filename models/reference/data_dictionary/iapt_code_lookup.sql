with ranked as (
    select
        code_set_name,
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
        source_effective_from_at as definition_updated_at,
        -- A code set can draw on more than one UKHFD attribute; prefer the current one.
        row_number() over (
            partition by code_set_name, code
            order by
                is_currently_valid desc,
                source_effective_from_at desc nulls last,
                source_imported_at desc,
                source_code_set_name
        ) as definition_rank
    from {{ ref('iapt_code_lookup_history') }}
    where is_latest_definition
)

select * exclude definition_rank
from ranked
where definition_rank = 1
