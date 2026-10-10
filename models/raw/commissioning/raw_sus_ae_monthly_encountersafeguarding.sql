{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterSafeguarding \ndbt: source(''sus_ae_monthly'', ''EncounterSafeguarding'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Sequence_Number -> sequence_number\n  Code -> code\n  Is_Approved -> is_approved"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "Is_Approved" as is_approved
from {{ source('sus_ae_monthly', 'EncounterSafeguarding') }}
