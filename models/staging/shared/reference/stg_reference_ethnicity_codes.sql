{{
    config(materialized = 'table')
}}

select
    code,
    term,
    -- The source maps 92391000000108 |British or mixed British - ethnic category
    -- 2001 census| to Mixed: Other Mixed. It is the 2001 census White British
    -- category (NHS ethnic category A; its only SNOMED child is White British -
    -- ethnic category 2001 census), so it is remapped to White: British here.
    -- Remove once DATA_LAKE__NCL.TERMINOLOGY.ETHNICITY_CODES is corrected.
    iff(code = '92391000000108', 'White', category) as category,
    iff(code = '92391000000108', 'White: British', subcategory) as subcategory,
    iff(code = '92391000000108', 'British', granular) as granular,
    deprioritise_flag,
    preference_rank
    -- Excluded (display/sorting only):
    -- category_sort,
    -- display_sort_key
from {{ ref('raw_reference_ethnicity_codes') }}
qualify row_number() over (partition by code order by term) = 1
