{{
    config(
        description="Raw layer (PSS reference tables including Service Line and National Programme Code). 1:1 passthrough with cleaned column names. \nSource: UKHFD.Prescribed_Specialised_Services.dim_Summary_Of_ID_Code_Sets \ndbt: source(''ukhfd_pss'', ''summary_of_id_code_sets'') \nColumns:\n  ADMIMETH_APC_Only -> admimeth_apc_only\n  ATTENDED_NAC_Only -> attended_nac_only\n  CLASSPAT_APC_Only -> classpat_apc_only\n  CSNum_Use_Of_Equal_Sign_Exclusion_APC_Only -> cs_num_use_of_equal_sign_exclusion_apc_only\n  Commissioned_By -> commissioned_by\n  Created_Date -> created_date\n  File_Name -> file_name\n  Flag_Number -> flag_number\n  ICD10_And_OPCS4_Combination_Required -> icd10_and_opcs4_combination_required\n  ICD10_Qualifiers -> icd10_qualifiers\n  ICD10_Triggering_Position -> icd10_triggering_position\n  ICD10_Triggers_Count -> icd10_triggers_count\n  Import_Date -> import_date\n  MAINSPEF_Count -> mainspef_count\n  NPoC_Category -> npo_c_category\n  New_Service_Lines -> new_service_lines\n  No_Of_Identification_Rules -> no_of_identification_rules\n  OPCS4_And_OPCS4_Combination_Required -> opcs4_and_opcs4_combination_required\n  OPCS4_Triggering_Position -> opcs4_triggering_position\n  OPCS4_Triggers_Count -> opcs4_triggers_count\n  OPCS_Qualifiers -> opcs_qualifiers\n  PS_Flag -> ps_flag\n  Prescribed_Service_Line -> prescribed_service_line\n  Provider_Eligibility_Count -> provider_eligibility_count\n  Qualifying_Age -> qualifying_age\n  Qualifying_Age_Check -> qualifying_age_check\n  Qualifying_Gender -> qualifying_gender\n  REFSOURC_NAC_Only -> refsourc_nac_only\n  Service_Line_Hierarchy -> service_line_hierarchy\n  Setting -> setting\n  Sub_Rule_Number -> sub_rule_number\n  TRETSPEF_Count -> tretspef_count\n  Unique_Column -> unique_column"
    )
}}
select
    "ADMIMETH_APC_Only" as admimeth_apc_only,
    "ATTENDED_NAC_Only" as attended_nac_only,
    "CLASSPAT_APC_Only" as classpat_apc_only,
    "CSNum_Use_Of_Equal_Sign_Exclusion_APC_Only" as cs_num_use_of_equal_sign_exclusion_apc_only,
    "Commissioned_By" as commissioned_by,
    "Created_Date" as created_date,
    "File_Name" as file_name,
    "Flag_Number" as flag_number,
    "ICD10_And_OPCS4_Combination_Required" as icd10_and_opcs4_combination_required,
    "ICD10_Qualifiers" as icd10_qualifiers,
    "ICD10_Triggering_Position" as icd10_triggering_position,
    "ICD10_Triggers_Count" as icd10_triggers_count,
    "Import_Date" as import_date,
    "MAINSPEF_Count" as mainspef_count,
    "NPoC_Category" as npo_c_category,
    "New_Service_Lines" as new_service_lines,
    "No_Of_Identification_Rules" as no_of_identification_rules,
    "OPCS4_And_OPCS4_Combination_Required" as opcs4_and_opcs4_combination_required,
    "OPCS4_Triggering_Position" as opcs4_triggering_position,
    "OPCS4_Triggers_Count" as opcs4_triggers_count,
    "OPCS_Qualifiers" as opcs_qualifiers,
    "PS_Flag" as ps_flag,
    "Prescribed_Service_Line" as prescribed_service_line,
    "Provider_Eligibility_Count" as provider_eligibility_count,
    "Qualifying_Age" as qualifying_age,
    "Qualifying_Age_Check" as qualifying_age_check,
    "Qualifying_Gender" as qualifying_gender,
    "REFSOURC_NAC_Only" as refsourc_nac_only,
    "Service_Line_Hierarchy" as service_line_hierarchy,
    "Setting" as setting,
    "Sub_Rule_Number" as sub_rule_number,
    "TRETSPEF_Count" as tretspef_count,
    "Unique_Column" as unique_column
from {{ source('ukhfd_pss', 'summary_of_id_code_sets') }}
