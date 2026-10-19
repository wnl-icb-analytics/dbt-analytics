{{
    config(
        description="Raw layer (Mapping between LSOA and Sub ICB geographies). 1:1 passthrough with cleaned column names. \nSource: UKHFD.LSOA_To_Sub_ICB.dim_Map_SCD \ndbt: source(''ukhfd_lsoa_to_sub_icb'', ''dim_Map_SCD'') \nColumns:\n  LSOA_Code -> lsoa_code\n  Sub_ICB_Location_Code -> sub_icb_location_code\n  Sub_ICB_Location_ODS_Code -> sub_icb_location_ods_code\n  ICB_Code -> icb_code\n  ICB_ODS_Code -> icb_ods_code\n  Local_Authority_District_Code -> local_authority_district_code\n  In_Source_Data -> in_source_data\n  Effective_Snapshot_Date -> effective_snapshot_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "LSOA_Code" as lsoa_code,
    "Sub_ICB_Location_Code" as sub_icb_location_code,
    "Sub_ICB_Location_ODS_Code" as sub_icb_location_ods_code,
    "ICB_Code" as icb_code,
    "ICB_ODS_Code" as icb_ods_code,
    "Local_Authority_District_Code" as local_authority_district_code,
    "In_Source_Data" as in_source_data,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_lsoa_to_sub_icb', 'dim_Map_SCD') }}
