select
    concept_code
    , published_value as response_code
    , response_description
    , try_to_decimal(published_value, 38, 9) as numeric_response_value
    -- These meanings are attached to the concept and response together, never a number alone.
    , coalesce(lower(trim(response_description)) in (
        'unknown', 'not known', 'missing data', 'missing', 'missing value',
        'don''t know/missing', 'not applicable'
    ), false) as is_non_score_response
    , specification_version
    , source_row
from {{ ref('csds_assessment_scale_definitions') }}
where regexp_like(published_value, '[A-Za-z]+|-?[0-9]+([.][0-9]+)?')
qualify row_number() over (
    partition by concept_code, published_value
    order by specification_version desc, source_row
) = 1
