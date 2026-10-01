{{ config(materialized='table') }}

-- All histories in a build share this accepted-submission snapshot.
select
    nullif(trim(a.unique_submission_id), '') as submission_id
    , upper(nullif(trim(h.org_id_prov), '')) as provider_organisation_code
    , h.reporting_period_start_date::date as reporting_period_start_date
    , h.reporting_period_end_date::date as reporting_period_end_date
    -- A non-integer month number stays null so its test fails rather than being rounded.
    , iff(regexp_like(trim(h.unique_month_id), '[0-9]+'), trim(h.unique_month_id)::number, null) as unique_month_id
    -- Formatted only when the submitted value is exactly 2.0 or 2.1; any other version keeps its raw text and
    -- fails the accepted-values test.
    , iff(h.dat_set_ver in (2.0, 2.1), to_varchar(h.dat_set_ver::number(2, 1)), to_varchar(h.dat_set_ver))
        as dataset_version
    , nullif(trim(h.file_type), '') as file_type
    , h.effective_from as source_file_received_at
    , h.date_time_dat_set_create as source_file_created_at
    , h.dmic_date_added as source_loaded_at
from {{ ref('raw_iapt_activesubmission') }} as a
left join {{ ref('raw_iapt_ids000header') }} as h
    on a.unique_submission_id = h.unique_submission_id
