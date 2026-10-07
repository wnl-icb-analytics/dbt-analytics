{{
    config(
        description="Raw layer: ODS ePCN core partner details - practice to PCN relationships, one row per relationship per daily snapshot.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.ODS_DSE.dim_Epcn_CorePartnerDetails \ndbt: source(''ukhfd_ods_dse'', ''pcn_core_partner_details'') \nColumns:\n  Partner_Organisation_Code -> partner_organisation_code\n  Partner_Name -> partner_name\n  Practice_Parent_Sub_ICB_Location_Code -> practice_parent_sub_icb_location_code\n  PCN_Code -> pcn_code\n  PCN_Name -> pcn_name\n  PCN_Parent_Sub_ICB_Location_Code -> pcn_parent_sub_icb_location_code\n  Practice_to_PCN_Relationship_Start_Date -> practice_to_pcn_relationship_start_date\n  Practice_to_PCN_Relationship_End_Date -> practice_to_pcn_relationship_end_date\n  Effective_Snapshot_Date -> effective_snapshot_date\n  DataSourceFileForThisSnapshot_Version -> data_source_file_for_this_snapshot_version"
    )
}}
select
    "Partner_Organisation_Code" as partner_organisation_code,
    "Partner_Name" as partner_name,
    "Practice_Parent_Sub_ICB_Location_Code" as practice_parent_sub_icb_location_code,
    "PCN_Code" as pcn_code,
    "PCN_Name" as pcn_name,
    "PCN_Parent_Sub_ICB_Location_Code" as pcn_parent_sub_icb_location_code,
    "Practice_to_PCN_Relationship_Start_Date" as practice_to_pcn_relationship_start_date,
    "Practice_to_PCN_Relationship_End_Date" as practice_to_pcn_relationship_end_date,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as data_source_file_for_this_snapshot_version
from {{ source('ukhfd_ods_dse', 'pcn_core_partner_details') }}
