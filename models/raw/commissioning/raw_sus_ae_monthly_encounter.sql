{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.Encounter \ndbt: source(''sus_ae_monthly'', ''Encounter'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  RowID -> row_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "RowID" as row_id,
    "SK_SUSDataMartID" as sk_sus_data_mart_id,
    "ActivityPeriod" as activity_period
from {{ source('sus_ae_monthly', 'Encounter') }}
