{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterBillingAttribution \ndbt: source(''sus_ae_monthly'', ''EncounterBillingAttribution'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Provider_CommissionerCode -> provider_commissioner_code\n  Provider_PracticeCode -> provider_practice_code\n  SUS_CommissionerCode_GPPractice -> sus_commissioner_code_gp_practice\n  SUS_CommissionerCode_Residence -> sus_commissioner_code_residence\n  SUS_PracticeCode -> sus_practice_code\n  SOLLIS_CommissionerCode -> sollis_commissioner_code\n  DWH_CommissionerCode -> dwh_commissioner_code\n  DWH_PracticeCode -> dwh_practice_code\n  Provider_GMPCode -> provider_gmp_code\n  SK_CommissionerID_DWH_Commissioner -> sk_commissioner_id_dwh_commissioner\n  SK_ServiceProviderID_DWH_Practice -> sk_service_provider_id_dwh_practice\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Provider_CommissionerCode" as provider_commissioner_code,
    "Provider_PracticeCode" as provider_practice_code,
    "SUS_CommissionerCode_GPPractice" as sus_commissioner_code_gp_practice,
    "SUS_CommissionerCode_Residence" as sus_commissioner_code_residence,
    "SUS_PracticeCode" as sus_practice_code,
    "SOLLIS_CommissionerCode" as sollis_commissioner_code,
    "DWH_CommissionerCode" as dwh_commissioner_code,
    "DWH_PracticeCode" as dwh_practice_code,
    "Provider_GMPCode" as provider_gmp_code,
    "SK_CommissionerID_DWH_Commissioner" as sk_commissioner_id_dwh_commissioner,
    "SK_ServiceProviderID_DWH_Practice" as sk_service_provider_id_dwh_practice,
    "ActivityPeriod" as activity_period
from {{ source('sus_ae_monthly', 'EncounterBillingAttribution') }}
