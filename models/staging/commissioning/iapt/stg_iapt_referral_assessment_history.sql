{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    pre_hook="{{ source_history_warehouse() }}",
    post_hook=["{{ iapt_remove_withdrawn_submissions() }}", "{{ source_history_warehouse(restore=true) }}"]
) }}

-- The warehouse stores the submitted completion time on a placeholder date; only the time is kept.
select
    s.submission_id
    , r.unique_id_ids606::varchar as source_row_id
    , upper(trim(r.org_id_provider)) as provider_organisation_code
    , nullif(trim(r.person_id), '') as person_id
    , nullif(trim(r.unique_service_request_id), '') as referral_id
    , nullif(trim(r.pathway_id), '') as pathway_id
    , nullif(trim(r.coded_ass_tool_type), '') as coded_ass_tool_type
    , nullif(trim(r.pers_score), '') as pers_score
    , r.ass_tool_comp_date::date as ass_tool_comp_date
    , r.ass_tool_comp_time::time as ass_tool_comp_time
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.unique_month_id
    , s.dataset_version
    , s.file_type
    , r.effective_from as source_file_received_at
    , r.dmic_date_added as source_loaded_at
from {{ ref('raw_iapt_ids606codedscoreassessmentrefer') }} as r
inner join {{ ref('stg_iapt_activesubmission') }} as s
    on r.unique_submission_id = s.submission_id
where s.reporting_period_end_date is not null
    and s.provider_organisation_code is not null
    and {{ iapt_unseen_submission('s.submission_id') }}
