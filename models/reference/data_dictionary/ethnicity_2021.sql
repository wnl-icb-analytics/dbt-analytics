-- ONS Census 2021 ethnic group labels for each NHS ethnic category code.
-- Source: ONS "Ethnic group variable: Census 2021" (20-category classification).
-- 2021 Gypsy or Irish Traveller and Roma fall within C; Arab falls within S.
with ons_2021_mapping as (
    select *
    from (
        values
            ('A', 'White: English, Welsh, Scottish, Northern Irish or British', 'White'),
            ('B', 'White: Irish', 'White'),
            ('C', 'White: Other White', 'White'),
            ('D', 'Mixed or Multiple ethnic groups: White and Black Caribbean', 'Mixed or Multiple ethnic groups'),
            ('E', 'Mixed or Multiple ethnic groups: White and Black African', 'Mixed or Multiple ethnic groups'),
            ('F', 'Mixed or Multiple ethnic groups: White and Asian', 'Mixed or Multiple ethnic groups'),
            ('G', 'Mixed or Multiple ethnic groups: Other Mixed or Multiple ethnic groups', 'Mixed or Multiple ethnic groups'),
            ('H', 'Asian, Asian British or Asian Welsh: Indian', 'Asian, Asian British or Asian Welsh'),
            ('J', 'Asian, Asian British or Asian Welsh: Pakistani', 'Asian, Asian British or Asian Welsh'),
            ('K', 'Asian, Asian British or Asian Welsh: Bangladeshi', 'Asian, Asian British or Asian Welsh'),
            ('L', 'Asian, Asian British or Asian Welsh: Other Asian', 'Asian, Asian British or Asian Welsh'),
            ('M', 'Black, Black British, Black Welsh, Caribbean or African: Caribbean', 'Black, Black British, Black Welsh, Caribbean or African'),
            ('N', 'Black, Black British, Black Welsh, Caribbean or African: African', 'Black, Black British, Black Welsh, Caribbean or African'),
            ('P', 'Black, Black British, Black Welsh, Caribbean or African: Other Black', 'Black, Black British, Black Welsh, Caribbean or African'),
            ('R', 'Asian, Asian British or Asian Welsh: Chinese', 'Asian, Asian British or Asian Welsh'),
            ('S', 'Other ethnic group: Any other ethnic group', 'Other ethnic group'),
            ('Z', 'Not stated', 'Not stated'),
            ('99', 'Not known', 'Not known')
    ) as mapping (ethnic_category_code, ethnicity_2021_category, ethnicity_2021_broad_group)
)

-- Driven from the NHS code list so an unmapped code surfaces as a null category.
select
    nhs.code as ethnic_category_code,
    mapping.ethnicity_2021_category,
    mapping.ethnicity_2021_broad_group
from {{ ref('nhs_dd_ethnic_category') }} as nhs
left join ons_2021_mapping as mapping
    on nhs.code = mapping.ethnic_category_code
