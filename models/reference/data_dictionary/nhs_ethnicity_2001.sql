with category_codes as (
    select
        ethnic_category_code as ethnicity_2001_code,
        description as source_description
    from {{ ref('stg_dictionary_dbo_ethnicitycode') }}
    where ethnicity_code_type = 'EthnicCategoryCode'
        -- Starred codes are alternative dictionary entries, not national codes.
        and ethnic_category_code not like '%*'
),
normalised_labels as (
    select
        ethnicity_2001_code,
        case
            when ethnicity_2001_code = 'Z' then 'Not stated'
            when ethnicity_2001_code = '99' then 'Not known'
            else source_description
        end as ethnicity_2001_detailed_description
    from category_codes
)
select
    ethnicity_2001_code,
    ethnicity_2001_detailed_description,
    case
        when ethnicity_2001_code in ('Z', '99') then ethnicity_2001_detailed_description
        else split_part(ethnicity_2001_detailed_description, ':', 1)
    end as ethnicity_2001_broad_group
from normalised_labels
