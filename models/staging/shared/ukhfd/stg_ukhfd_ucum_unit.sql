select
    nullif(trim(code), '') as code
    , nullif(trim(descriptive_name), '') as description
    , nullif(trim(synonym), '') as synonym
    , nullif(trim(kind_of_quantity), '') as quantity_name
    , nullif(trim(definition), '') as definition
    , nullif(trim(status), '') as status
    , code_system
    , dimension
    , date_created as definition_created_date
    , date_revised as definition_revised_date
    , in_source_data = 1 as is_in_latest_source
    , is_latest = 1 as is_latest_definition
    , effective_from as source_effective_from_at
    , effective_to as source_effective_to_at
    , import_date as source_imported_at
    , created_date as source_created_at
from {{ ref('raw_ukhfd_other_ucum_units') }}
where nullif(trim(code), '') is not null
