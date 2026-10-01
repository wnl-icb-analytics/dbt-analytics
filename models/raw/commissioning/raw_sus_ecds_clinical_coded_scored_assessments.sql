{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.coded_scored_assessments \ndbt: source(''sus_ecds'', ''clinical.coded_scored_assessments'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CODED_SCORED_ASSESSMENTS_ID -> coded_scored_assessments_id\n  tool_type.code -> tool_type_code\n  tool_type.is_code_approved -> tool_type_is_code_approved\n  person_score -> person_score\n  validation_timestamp -> validation_timestamp\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CODED_SCORED_ASSESSMENTS_ID" as coded_scored_assessments_id,
    "tool_type.code" as tool_type_code,
    "tool_type.is_code_approved" as tool_type_is_code_approved,
    "person_score" as person_score,
    "validation_timestamp" as validation_timestamp,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'clinical.coded_scored_assessments') }}
