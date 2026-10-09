{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterAttribution \ndbt: source(''sus_ae_monthly'', ''EncounterAttribution'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  ProviderCode_SollisDerived -> provider_code_sollis_derived\n  CommissionerCode_SollisDerived -> commissioner_code_sollis_derived\n  ProviderCode_ProviderDerived -> provider_code_provider_derived\n  CommissionerCode_ProviderDerived -> commissioner_code_provider_derived\n  CommissionerCode_DerivedFromGPCode -> commissioner_code_derived_from_gp_code\n  CommissionerCode_DerivedFromPracticeCode -> commissioner_code_derived_from_practice_code\n  SUSCommissionerCode_DerivedFromResidence -> sus_commissioner_code_derived_from_residence\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "ProviderCode_SollisDerived" as provider_code_sollis_derived,
    "CommissionerCode_SollisDerived" as commissioner_code_sollis_derived,
    "ProviderCode_ProviderDerived" as provider_code_provider_derived,
    "CommissionerCode_ProviderDerived" as commissioner_code_provider_derived,
    "CommissionerCode_DerivedFromGPCode" as commissioner_code_derived_from_gp_code,
    "CommissionerCode_DerivedFromPracticeCode" as commissioner_code_derived_from_practice_code,
    "SUSCommissionerCode_DerivedFromResidence" as sus_commissioner_code_derived_from_residence,
    "ActivityPeriod" as activity_period
from {{ source('sus_ae_monthly', 'EncounterAttribution') }}
