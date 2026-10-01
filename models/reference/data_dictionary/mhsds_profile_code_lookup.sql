select
    code_set_name
    , code
    , description
    , short_description
    , category
    , notes
    , valid_from_date
    , valid_to_date
    , is_currently_valid
    , source_code_set_name
    , source_imported_at
    , source_effective_from_at as definition_updated_at
from {{ ref('mhsds_profile_code_lookup_history') }}
where is_latest_definition
