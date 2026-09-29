select
    a.unique_submission_id
    , h.organisation_code_code_of_provider as provider_organisation_code
    , h.reporting_period_start_date::date as reporting_period_start_date
    , h.reporting_period_end_date::date as reporting_period_end_date
    , h.data_set_version_number as csds_version
    , h.effective_from as source_file_received_at
    , h.date_and_time_data_set_created as source_file_created_at
    , h.upload_date_time as source_uploaded_at
    , h.file_type
from {{ ref('raw_csds_activesubmission') }} as a
left join {{ ref('raw_csds_cyp000header') }} as h
    on a.unique_submission_id = h.unique_submission_id
