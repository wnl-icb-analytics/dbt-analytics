{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.attendance.referred_to \ndbt: source(''sus_ecds'', ''attendance.referred_to'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  REFERRED_TO_ID -> referred_to_id\n  service -> service\n  is_code_approved -> is_code_approved\n  request_date -> request_date\n  request_time -> request_time\n  assessment_date -> assessment_date\n  assessment_time -> assessment_time\n  dmicImportLogId -> dmic_import_log_id\n  request_timestamp -> request_timestamp\n  assessment_timestamp -> assessment_timestamp"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "REFERRED_TO_ID" as referred_to_id,
    "service" as service,
    "is_code_approved" as is_code_approved,
    "request_date" as request_date,
    "request_time" as request_time,
    "assessment_date" as assessment_date,
    "assessment_time" as assessment_time,
    "dmicImportLogId" as dmic_import_log_id,
    "request_timestamp" as request_timestamp,
    "assessment_timestamp" as assessment_timestamp
from {{ source('sus_ecds', 'attendance.referred_to') }}
