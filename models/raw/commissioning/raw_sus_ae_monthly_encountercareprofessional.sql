{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterCareProfessional \ndbt: source(''sus_ae_monthly'', ''EncounterCareProfessional'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Sequence_Number -> sequence_number\n  Code -> code\n  SK_Issuer_Code_ID -> sk_issuer_code_id\n  Tier -> tier\n  Discharge_Responsibility_Indicator -> discharge_responsibility_indicator\n  Is_Approved -> is_approved"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "SK_Issuer_Code_ID" as sk_issuer_code_id,
    "Tier" as tier,
    "Discharge_Responsibility_Indicator" as discharge_responsibility_indicator,
    "Is_Approved" as is_approved
from {{ source('sus_ae_monthly', 'EncounterCareProfessional') }}
