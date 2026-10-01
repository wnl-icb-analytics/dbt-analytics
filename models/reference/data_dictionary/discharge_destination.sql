with ranked as (
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
        source_effective_from_at as definition_updated_at,
        row_number() over (
            partition by code
            order by
                -- Prefer the modern code set even when its code has retired.
                source_code_set_name = 'Destination_Of_Discharge' desc,
                source_effective_from_at desc nulls last,
                source_imported_at desc nulls last,
                source_unique_key desc
        ) as definition_rank
    from {{ ref('discharge_destination_history') }}
    where is_latest_definition
)

select * exclude definition_rank
from ranked
where definition_rank = 1
