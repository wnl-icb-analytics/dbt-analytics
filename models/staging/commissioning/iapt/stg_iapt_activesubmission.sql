{{ config(materialized='table') }}

-- All histories in a build share this accepted-submission snapshot.
select
    nullif(trim(a.unique_submission_id), '') as submission_id
    , h.provider_organisation_code
    , h.reporting_period_start_date
    , h.reporting_period_end_date
    , h.unique_month_id
    , h.dataset_version
    , h.file_type
    , h.source_file_received_at
    , h.source_file_created_at
    , h.source_loaded_at
from {{ ref('raw_iapt_activesubmission') }} as a
left join {{ ref('stg_iapt_submission_header') }} as h
    on nullif(trim(a.unique_submission_id), '') = h.submission_id
