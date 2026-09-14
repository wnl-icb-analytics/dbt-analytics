{{
    config(
        description="Raw layer (Rapid Cancer Registration Dataset (RCRD)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.RCRD.ActiveSubmission \ndbt: source(''rcrd'', ''ActiveSubmission'') \nColumns:\n  UniqSubmissionId -> uniq_submission_id"
    )
}}
select
    "UniqSubmissionId" as uniq_submission_id
from {{ source('rcrd', 'ActiveSubmission') }}
