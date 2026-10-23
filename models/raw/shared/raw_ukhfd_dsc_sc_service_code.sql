{{
    config(
        description="Raw layer: NHS England specialised service codes from the service code workbook, with UKHFD snapshot history.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.DSC_SC.dim_Service_Code_Lookup_SCD \ndbt: source(''ukhfd_dsc_sc'', ''service_code'') \nColumns:\n  Service_Code -> service_code\n  Service_Code_Description -> service_code_description\n  Service_Code_Including_Description -> service_code_including_description\n  Service_Category_Description -> service_category_description\n  NPOC_Category_And_CRG -> npoc_category_and_crg\n  Highly_Specialised_Service -> highly_specialised_service\n  Identified_By_PS_Operational_Tool -> identified_by_ps_operational_tool\n  Identified_By_PS_Operational_Tool_Year_Ending -> identified_by_ps_operational_tool_year_ending\n  Notes -> notes\n  Effective_From_Taken_From_Source_File -> effective_from_taken_from_source_file\n  Effective_To_Taken_From_Source_File -> effective_to_taken_from_source_file\n  Is_Current_Taken_From_Source_File -> is_current_taken_from_source_file\n  ICB_Delegation_Status -> icb_delegation_status\n  Grouping -> grouping\n  Amended_Record_Indicator -> amended_record_indicator\n  In_Source_Data -> in_source_data\n  Effective_Snapshot_Date -> effective_snapshot_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Service_Code" as service_code,
    "Service_Code_Description" as service_code_description,
    "Service_Code_Including_Description" as service_code_including_description,
    "Service_Category_Description" as service_category_description,
    "NPOC_Category_And_CRG" as npoc_category_and_crg,
    "Highly_Specialised_Service" as highly_specialised_service,
    "Identified_By_PS_Operational_Tool" as identified_by_ps_operational_tool,
    "Identified_By_PS_Operational_Tool_Year_Ending" as identified_by_ps_operational_tool_year_ending,
    "Notes" as notes,
    "Effective_From_Taken_From_Source_File" as effective_from_taken_from_source_file,
    "Effective_To_Taken_From_Source_File" as effective_to_taken_from_source_file,
    "Is_Current_Taken_From_Source_File" as is_current_taken_from_source_file,
    "ICB_Delegation_Status" as icb_delegation_status,
    "Grouping" as grouping,
    "Amended_Record_Indicator" as amended_record_indicator,
    "In_Source_Data" as in_source_data,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dsc_sc', 'service_code') }}
