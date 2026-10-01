{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.system.record.copy_recipients \ndbt: source(''sus_ecds'', ''system.record.copy_recipients'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  COPY_RECIPIENTS_ID -> copy_recipients_id\n  copy_recipients -> copy_recipients\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "COPY_RECIPIENTS_ID" as copy_recipients_id,
    "copy_recipients" as copy_recipients,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'system.record.copy_recipients') }}
