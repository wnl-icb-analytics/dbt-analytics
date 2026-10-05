select
    upper(trim(partner_organisation_code)) as practice_code
    , partner_name as practice_name
    , practice_parent_sub_icb_location_code as practice_sub_icb_code
    , upper(trim(pcn_code)) as pcn_code
    , pcn_name
    , pcn_parent_sub_icb_location_code as pcn_sub_icb_code
    , practice_to_pcn_relationship_start_date as relationship_start_date
    , practice_to_pcn_relationship_end_date as relationship_end_date
    , effective_snapshot_date as snapshot_date
    , datasourcefileforthissnapshot_version as snapshot_file_version
from {{ ref('raw_ukhfd_pcn_core_partner_details') }}
