with categorised as (
    select
        *,
        -- UKHFD's newest revision can drop a category an earlier one published: every current discharge reason lost
        -- its I101250 group in the 2022 revision, with no other change. Keep the newest category supplied.
        last_value(category) ignore nulls over (
            partition by code_set_name, source_code_set_name, code
            order by source_effective_from_at nulls first, source_imported_at
            rows between unbounded preceding and unbounded following
        ) as latest_published_category
    from {{ ref('iapt_code_lookup_history') }}
),

ranked as (
    select
        code_set_name,
        code,
        description,
        short_description,
        latest_published_category as category,
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
    from categorised
    where is_latest_definition
)

select * exclude definition_rank
from ranked
where definition_rank = 1
