{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterInvestigation \ndbt: source(''sus_ae_monthly'', ''EncounterInvestigation'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  InvestigationNumber -> investigation_number\n  InvestigationCode -> investigation_code\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "InvestigationNumber" as investigation_number,
    "InvestigationCode" as investigation_code,
    "ActivityPeriod" as activity_period
from {{ source('sus_ae_monthly', 'EncounterInvestigation') }}
