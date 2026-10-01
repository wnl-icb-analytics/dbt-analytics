{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.patient.mental_health_act_legal_status \ndbt: source(''sus_ecds'', ''patient.mental_health_act_legal_status'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  MENTAL_HEALTH_ACT_LEGAL_STATUS_ID -> mental_health_act_legal_status_id\n  classification -> classification\n  start_date -> start_date\n  start_time -> start_time\n  expiry_date -> expiry_date\n  expiry_time -> expiry_time\n  dmicImportLogId -> dmic_import_log_id\n  assignment_timestamp -> assignment_timestamp\n  expiry_timestamp -> expiry_timestamp"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "MENTAL_HEALTH_ACT_LEGAL_STATUS_ID" as mental_health_act_legal_status_id,
    "classification" as classification,
    "start_date" as start_date,
    "start_time" as start_time,
    "expiry_date" as expiry_date,
    "expiry_time" as expiry_time,
    "dmicImportLogId" as dmic_import_log_id,
    "assignment_timestamp" as assignment_timestamp,
    "expiry_timestamp" as expiry_timestamp
from {{ source('sus_ecds', 'patient.mental_health_act_legal_status') }}
