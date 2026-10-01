{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.comorbidities \ndbt: source(''sus_ecds'', ''clinical.comorbidities'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  COMORBIDITIES_ID -> comorbidities_id\n  code -> code\n  is_code_approved -> is_code_approved\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "COMORBIDITIES_ID" as comorbidities_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'clinical.comorbidities') }}
