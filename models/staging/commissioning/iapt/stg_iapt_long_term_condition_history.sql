{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    post_hook="{{ iapt_remove_withdrawn_submissions() }}"
) }}

select
    s.submission_id
    , r.unique_id_ids602::varchar as source_row_id
    , upper(trim(r.org_id_provider)) as provider_organisation_code
    , nullif(trim(r.person_id), '') as person_id
    , nullif(trim(r.unique_service_request_id), '') as referral_id
    , nullif(trim(r.pathway_id), '') as pathway_id
    , nullif(upper(trim(r.find_scheme_in_use)), '') as find_scheme_in_use
    , nullif(trim(r.long_term_condition), '') as long_term_condition
    , nullif(upper(trim(r.validated_long_term_condition_code)), '') as validated_long_term_condition_code
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.unique_month_id
    , s.dataset_version
    , s.file_type
    , r.effective_from as source_file_received_at
    , r.dmic_date_added as source_loaded_at
from {{ ref('raw_iapt_ids602longtermcondition') }} as r
inner join {{ ref('stg_iapt_activesubmission') }} as s
    on r.unique_submission_id = s.submission_id
where s.reporting_period_end_date is not null
    and s.provider_organisation_code is not null
    and {{ iapt_unseen_submission('s.submission_id') }}
