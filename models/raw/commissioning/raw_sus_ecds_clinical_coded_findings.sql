{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.coded_findings \ndbt: source(''sus_ecds'', ''clinical.coded_findings'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CODED_FINDINGS_ID -> coded_findings_id\n  code -> code\n  is_code_approved -> is_code_approved\n  timestamp -> timestamp\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CODED_FINDINGS_ID" as coded_findings_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "timestamp" as timestamp,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'clinical.coded_findings') }}
