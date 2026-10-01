select
    'ISO 639-1' as source_code_set_name
    , upper(nullif(trim(iso_639_1_code), '')) as code
    , nullif(trim(english_name_of_language), '') as description
    , null::varchar as short_description
    , null::varchar as category
    , null::varchar as notes
    , null::date as valid_from_date
    , null::date as valid_to_date
    , in_source_data = 1 as is_currently_valid
    , upper(nullif(trim(iso_639_1_code), '')) as source_unique_key
    , import_date as source_imported_at
    , created_date as source_created_at
    , is_latest = 1 as is_latest_definition
    , effective_from as source_effective_from_at
    , effective_to as source_effective_to_at
from {{ ref('raw_ukhfd_other_iso_language_codes') }}
where nullif(trim(iso_639_1_code), '') is not null
qualify row_number() over (
    partition by upper(trim(iso_639_1_code)), effective_from
    order by iso_639_2_code
) = 1
