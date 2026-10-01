{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.patient.social_and_personal_circumstances \ndbt: source(''sus_ecds'', ''patient.social_and_personal_circumstances'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  SOCIAL_AND_PERSONAL_CIRCUMSTANCES_ID -> social_and_personal_circumstances_id\n  code -> code\n  recorded_timestamp -> recorded_timestamp\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SOCIAL_AND_PERSONAL_CIRCUMSTANCES_ID" as social_and_personal_circumstances_id,
    "code" as code,
    "recorded_timestamp" as recorded_timestamp,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'patient.social_and_personal_circumstances') }}
