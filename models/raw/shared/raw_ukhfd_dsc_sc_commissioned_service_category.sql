{{
    config(
        description="Raw layer: Commissioned Service Category Code values and their UKHFD snapshot history.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.DSC_SC.dim_Commissioned_Service_Cat_Code_Lookup_SCD \ndbt: source(''ukhfd_dsc_sc'', ''commissioned_service_category'') \nColumns:\n  Commissioned_Service_Category_Code -> commissioned_service_category_code\n  Commissioned_Service_Category_Code_Description -> commissioned_service_category_code_description\n  In_Source_Data -> in_source_data\n  Effective_Snapshot_Date -> effective_snapshot_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Commissioned_Service_Category_Code" as commissioned_service_category_code,
    "Commissioned_Service_Category_Code_Description" as commissioned_service_category_code_description,
    "In_Source_Data" as in_source_data,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dsc_sc', 'commissioned_service_category') }}
