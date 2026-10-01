{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.investigations.snomed \ndbt: source(''sus_ecds'', ''clinical.investigations.snomed'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  SNOMED_ID -> snomed_id\n  code -> code\n  is_code_approved -> is_code_approved\n  equivalent_ae_code -> equivalent_ae_code\n  date -> date\n  time -> time\n  dmicImportLogId -> dmic_import_log_id\n  timestamp -> timestamp"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SNOMED_ID" as snomed_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "equivalent_ae_code" as equivalent_ae_code,
    "date" as date,
    "time" as time,
    "dmicImportLogId" as dmic_import_log_id,
    "timestamp" as timestamp
from {{ source('sus_ecds', 'clinical.investigations.snomed') }}
