select
    concept_code
    , specification_version
    , published_value as response_code
    , response_description
    , try_to_decimal(published_value, 38, 9) as numeric_response_value
    , is_non_score_response
from {{ ref('iapt_assessment_scale_definitions') }}
-- Numeric ranges stay in iapt_assessment_scale.
where not regexp_like(published_value, '-?[0-9]+([.][0-9]+)? *- *-?[0-9]+([.][0-9]+)?')
