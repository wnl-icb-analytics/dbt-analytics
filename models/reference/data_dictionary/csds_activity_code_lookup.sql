select
    code_set_name
    , source_code_set_name
    , code
    , description
    , short_description
    , category
    , notes
    , valid_from_date
    , valid_to_date
    , is_currently_valid
    , source_imported_at
    , source_effective_from_at as definition_updated_at
from {{ ref('csds_activity_code_lookup_history') }}
where is_latest_definition
qualify row_number() over (
    partition by code_set_name, code
    order by
        source_code_set_name = 'Community_Care_Activity_Type' desc
        , source_code_set_name = 'Referral_Closure_Reason' desc
        , source_effective_from_at desc nulls last
        , source_imported_at desc nulls last
        , source_code_set_name
) = 1
