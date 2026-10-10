{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.ActiveSubmission \ndbt: source(''iapt'', ''ActiveSubmission'') \nColumns:\n  UniqueSubmissionID -> unique_submission_id"
    )
}}
select
    "UniqueSubmissionID" as unique_submission_id
from {{ source('iapt', 'ActiveSubmission') }}
