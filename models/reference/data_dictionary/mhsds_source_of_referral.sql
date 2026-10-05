-- A selected definition without a category falls back to the newest category published for the code, e.g. H2,
-- whose preferred MHSDS-list definition has none.
with fallback_category as (
    select code, category
    from {{ ref('mhsds_source_of_referral_history') }}
    where category is not null
    qualify row_number() over (
        partition by code
        order by
            source_effective_from_at desc nulls last,
            source_code_set_name = 'Source_Of_Referral_For_Mental_Health_Services_Data_Set' desc,
            source_imported_at desc,
            source_unique_key
    ) = 1
),

ranked as (
    select
        h.code,
        description,
        short_description,
        coalesce(h.category, f.category) as category,
        notes,
        valid_from_date,
        valid_to_date,
        is_currently_valid,
        source_code_set_name,
        source_imported_at,
        source_effective_from_at as definition_updated_at,
        row_number() over (
            partition by h.code
            order by
                is_currently_valid desc,
                source_code_set_name = 'Source_Of_Referral_For_Mental_Health_Services_Data_Set' desc,
                source_effective_from_at desc nulls last,
                source_imported_at desc
        ) as definition_rank
    from {{ ref('mhsds_source_of_referral_history') }} as h
    left join fallback_category as f on h.code = f.code
    where is_latest_definition
)

select * exclude definition_rank
from ranked
where definition_rank = 1
