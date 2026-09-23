select
    unique_column as source_record_id
    , sheet_name
    , regexp_replace(sheet_name, '^[0-9.]+ ', '') as code_set_name
    , snomed_code
    , coalesce(nullif(trim(snomed_uk_preferred_term), ''), nullif(trim(snomed_term), ''), nullif(trim(snomed_description), '')) as preferred_term
    , ecds_description
    , ecds_group1
    , ecds_group2
    , ecds_group3
    , notes
    , notes_for_completing_the_observation_value_field as value_guidance
    , valid_from
    , valid_to
    , file_name as source_file_name
    , try_to_timestamp_ntz(left(file_name, 14), 'YYYYMMDDHH24MISS') as source_file_at
    , import_date as source_imported_at
from {{ ref('raw_ukhfd_ecds_code_sets') }}
