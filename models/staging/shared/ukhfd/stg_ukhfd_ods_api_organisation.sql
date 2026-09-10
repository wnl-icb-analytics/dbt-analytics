select
    register_id as source_register_id
    , nullif(upper(trim(org_id_extension)), '') as organisation_code
    , nullif(trim(name), '') as organisation_name
    , org_id_root as organisation_identifier_root
    , org_id_assigning_authority_name as organisation_assigning_authority_name
    , org_record_class as organisation_record_class
    , status as organisation_status
    , in_source_data = 1 as is_in_latest_source
    , is_latest = 1 as is_latest_definition
    , last_change_date as source_last_changed_date
    , effective_from as source_effective_from_at
    , effective_to as source_effective_to_at
from {{ ref('raw_ukhfd_organisation') }}
