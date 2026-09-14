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
                is_currently_valid desc,
                source_code_set_name = 'Source_Of_Referral_For_Mental_Health_Services_Data_Set' desc,
                source_effective_from_at desc nulls last,
                source_imported_at desc
        ) as definition_rank
    from {{ ref('mhsds_source_of_referral_history') }}
    where is_latest_definition
)

select * exclude definition_rank
from ranked
where definition_rank = 1
