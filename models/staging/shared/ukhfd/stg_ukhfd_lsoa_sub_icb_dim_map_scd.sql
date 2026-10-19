select
    lsoa_code,
    sub_icb_location_code,
    sub_icb_location_ods_code,
    icb_code,
    icb_ods_code,
    local_authority_district_code,
    in_source_data::number as in_source_data,
    effective_snapshot_date,
    is_latest,
    effective_from,
    effective_to

from {{ ref('raw_ukhfd_lsoa_sub_icb_dim_map_scd') }}