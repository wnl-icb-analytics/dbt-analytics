{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.attendance.expected_treatment_times \ndbt: source(''sus_ecds'', ''attendance.expected_treatment_times'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EXPECTED_TREATMENT_TIMES_ID -> expected_treatment_times_id\n  timestamp -> timestamp\n  allocated_timestamp -> allocated_timestamp\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EXPECTED_TREATMENT_TIMES_ID" as expected_treatment_times_id,
    "timestamp" as timestamp,
    "allocated_timestamp" as allocated_timestamp,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'attendance.expected_treatment_times') }}
