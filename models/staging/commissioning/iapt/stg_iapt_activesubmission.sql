{{ config(materialized='table') }}

-- All histories in a build share this accepted-submission snapshot.
select
    nullif(trim(a.unique_submission_id), '') as submission_id
    , upper(nullif(trim(h.org_id_prov), '')) as provider_organisation_code
    , h.reporting_period_start_date::date as reporting_period_start_date
    , h.reporting_period_end_date::date as reporting_period_end_date
    , try_to_number(h.unique_month_id) as unique_month_id
    , to_varchar(h.dat_set_ver::number(4, 1)) as dataset_version
    , nullif(trim(h.file_type), '') as file_type
    , h.effective_from as source_file_received_at
    , h.date_time_dat_set_create as source_file_created_at
    , h.dmic_date_added as source_loaded_at
from {{ ref('raw_iapt_activesubmission') }} as a
left join {{ ref('raw_iapt_ids000header') }} as h
    on a.unique_submission_id = h.unique_submission_id
