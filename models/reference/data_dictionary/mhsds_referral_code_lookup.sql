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
        row_number() over (
            partition by code_set_name, code
            order by
                is_currently_valid desc,
                source_code_set_name
                    = 'Referring_Care_Professional_Type_Mental_Health' desc,
                source_code_set_name
                    = 'Referring_Care_Professional_Staff_Group_For_Mental_Health' desc,
                source_effective_from_at desc nulls last,
                source_imported_at desc
        ) as definition_rank
    from {{ ref('mhsds_referral_code_lookup_history') }}
    where is_latest_definition
)

select * exclude definition_rank
from ranked
where definition_rank = 1
