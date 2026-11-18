select
    code as ethnicity_2001_code,
    description as ethnicity_2001_description,
    case
        when category is null then description
        else category || ': ' || description
    end as ethnicity_2001_detailed_description,
    -- Z (Not stated) and 99 (Not known) have no category.
    coalesce(category, description) as ethnicity_2001_broad_group
from {{ ref('nhs_dd_ethnic_category') }}
