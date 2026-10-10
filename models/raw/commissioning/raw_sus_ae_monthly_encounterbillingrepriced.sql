{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterBillingRepriced \ndbt: source(''sus_ae_monthly'', ''EncounterBillingRepriced'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_CostingAlgorithmID -> sk_costing_algorithm_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  Month_Of_Attendance -> month_of_attendance\n  Provider_Code -> provider_code\n  Purchaser_Code -> purchaser_code\n  Contract_Suffix -> contract_suffix\n  Base_Cost -> base_cost\n  MFF_Applied -> mff_applied\n  Total_Cost -> total_cost\n  POD_Description -> pod_description\n  Schedule_Description -> schedule_description\n  LocalCostCode -> local_cost_code\n  HRG_Code -> hrg_code\n  AE_Department_Type -> ae_department_type"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_CostingAlgorithmID" as sk_costing_algorithm_id,
    "SK_SUSDataMartID" as sk_sus_data_mart_id,
    "Month_Of_Attendance" as month_of_attendance,
    "Provider_Code" as provider_code,
    "Purchaser_Code" as purchaser_code,
    "Contract_Suffix" as contract_suffix,
    "Base_Cost" as base_cost,
    "MFF_Applied" as mff_applied,
    "Total_Cost" as total_cost,
    "POD_Description" as pod_description,
    "Schedule_Description" as schedule_description,
    "LocalCostCode" as local_cost_code,
    "HRG_Code" as hrg_code,
    "AE_Department_Type" as ae_department_type
from {{ source('sus_ae_monthly', 'EncounterBillingRepriced') }}
