{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterTreatment \ndbt: source(''sus_ae_monthly'', ''EncounterTreatment'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  TreatmentNumber -> treatment_number\n  TreatmentCode -> treatment_code\n  ProcedureDate -> procedure_date\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "TreatmentNumber" as treatment_number,
    "TreatmentCode" as treatment_code,
    "ProcedureDate" as procedure_date,
    "ActivityPeriod" as activity_period
from {{ source('sus_ae_monthly', 'EncounterTreatment') }}
