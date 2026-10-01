{{
    config(materialized = 'table')
}}

select
    code,
    term,
    category,
    subcategory,
    granular,
    deprioritise_flag,
    preference_rank
    -- Excluded (display/sorting only):
    -- category_sort,
    -- display_sort_key
from {{ ref('raw_reference_ethnicity_codes') }}
qualify row_number() over (partition by code order by term) = 1
