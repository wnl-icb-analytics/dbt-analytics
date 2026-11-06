{{
    config(
        description="Raw layer (LAS commissioning data). 1:1 passthrough with cleaned column names. \nSource: WNL_ServicesDataLocal.AMB.Log_ProcessingEvent \ndbt: source(''amb_wnl'', ''Log_ProcessingEvent'') \nColumns:\n  SK_RowID -> sk_row_id\n  BatchId -> batch_id\n  FileID -> file_id\n  StageID -> stage_id\n  ServiceCode -> service_code\n  ProfileCode -> profile_code\n  FileName -> file_name\n  OriginalFileName -> original_file_name\n  Message -> message\n  Status -> status\n  RowCount -> row_count\n  SourceColumnHeaders -> source_column_headers\n  ActualColCount -> actual_col_count\n  ExpectedColCount -> expected_col_count\n  LoaderJobName -> loader_job_name\n  DSPDisseminationDateTime -> dsp_dissemination_date_time\n  FileSize -> file_size\n  StartDate -> start_date\n  EndDate -> end_date\n  CreatedBy -> created_by\n  CreatedDateTime -> created_date_time\n  LastUpdatedBy -> last_updated_by\n  LastUpdatedDateTime -> last_updated_date_time\n  HeaderID -> header_id\n  IsReProcessed -> is_re_processed"
    )
}}
select
    "SK_RowID" as sk_row_id,
    "BatchId" as batch_id,
    "FileID" as file_id,
    "StageID" as stage_id,
    "ServiceCode" as service_code,
    "ProfileCode" as profile_code,
    "FileName" as file_name,
    "OriginalFileName" as original_file_name,
    "Message" as message,
    "Status" as status,
    "RowCount" as row_count,
    "SourceColumnHeaders" as source_column_headers,
    "ActualColCount" as actual_col_count,
    "ExpectedColCount" as expected_col_count,
    "LoaderJobName" as loader_job_name,
    "DSPDisseminationDateTime" as dsp_dissemination_date_time,
    "FileSize" as file_size,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "CreatedBy" as created_by,
    "CreatedDateTime" as created_date_time,
    "LastUpdatedBy" as last_updated_by,
    "LastUpdatedDateTime" as last_updated_date_time,
    "HeaderID" as header_id,
    "IsReProcessed" as is_re_processed
from {{ source('amb_wnl', 'Log_ProcessingEvent') }}
