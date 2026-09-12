{{
    config(
        description="Raw layer: Service line hierarchy information.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Prescribed_Specialised_Services.dim_Service_Line_Hierarchy \ndbt: source(''ukhfd_pss'', ''service_line_hierarchy'') \nColumns:\n  Service_Line -> service_line\n  Service_Line_Description -> service_line_description\n  Applicable_In_Setting_1 -> applicable_in_setting_1\n  Applicable_In_Setting_2 -> applicable_in_setting_2\n  APC_Episode_Spell_Hierarchies -> apc_episode_spell_hierarchies\n  APC_Percentage_Top_Up -> apc_percentage_top_up\n  NAC_Hierarchy -> nac_hierarchy\n  File_Name -> file_name\n  Import_Date -> import_date\n  Created_Date -> created_date"
    )
}}
select
    "Service_Line" as service_line,
    "Service_Line_Description" as service_line_description,
    "Applicable_In_Setting_1" as applicable_in_setting_1,
    "Applicable_In_Setting_2" as applicable_in_setting_2,
    "APC_Episode_Spell_Hierarchies" as apc_episode_spell_hierarchies,
    "APC_Percentage_Top_Up" as apc_percentage_top_up,
    "NAC_Hierarchy" as nac_hierarchy,
    "File_Name" as file_name,
    "Import_Date" as import_date,
    "Created_Date" as created_date
from {{ source('ukhfd_pss', 'service_line_hierarchy') }}
