select
    nullif(trim(service_code), '') as service_code
    , nullif(trim(service_code_description), '') as service_code_description
    , nullif(trim(service_code_including_description), '') as service_code_including_description
    , nullif(trim(service_category_description), '') as service_category_description
    , nullif(trim(npoc_category_and_crg), '') as npoc_category_and_crg
    , nullif(trim(highly_specialised_service), '') as highly_specialised_service
    , nullif(trim(identified_by_ps_operational_tool), '') as identified_by_ps_operational_tool
    , identified_by_ps_operational_tool_year_ending as identified_by_ps_operational_tool_year_end_date
    , nullif(trim(notes), '') as notes
    , effective_from_taken_from_source_file as effective_from_date
    , effective_to_taken_from_source_file as effective_to_date
    , trim(is_current_taken_from_source_file) = '1' as is_current
    , nullif(trim(icb_delegation_status), '') as icb_delegation_status
    , nullif(trim(grouping), '') as service_grouping
    , nullif(trim(amended_record_indicator), '') as amended_record_indicator
    , in_source_data = 1 as is_in_latest_source
    , is_latest = 1 as is_latest_definition
    , effective_snapshot_date as source_snapshot_date
    , effective_from as source_effective_from_at
    , effective_to as source_effective_to_at
from {{ ref('raw_ukhfd_dsc_sc_service_code') }}
