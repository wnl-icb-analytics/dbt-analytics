{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    pre_hook="{{ source_history_warehouse() }}",
    post_hook=["{{ iapt_remove_withdrawn_submissions() }}", "{{ source_history_warehouse(restore=true) }}"]
) }}

-- The provider-qualified local patient id stays here only to key the diagnosis in facts.
select
    s.submission_id
    , r.unique_id_ids601::varchar as source_row_id
    , upper(trim(r.org_id_provider)) as provider_organisation_code
    , nullif(trim(r.person_id), '') as person_id
    , nullif(trim(r.unique_local_patient_id), '') as unique_local_patient_id
    , nullif(upper(trim(r.diag_scheme_in_use)), '') as diag_scheme_in_use
    , nullif(trim(r.prev_diag), '') as prev_diag
    , r.diag_date::date as diag_date
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.unique_month_id
    , s.dataset_version
    , s.file_type
    , r.effective_from as source_file_received_at
    , r.dmic_date_added as source_loaded_at
from {{ ref('raw_iapt_ids601medhistprevdiag') }} as r
inner join {{ ref('stg_iapt_activesubmission') }} as s
    on r.unique_submission_id = s.submission_id
where s.reporting_period_end_date is not null
    and s.provider_organisation_code is not null
    and {{ iapt_unseen_submission('s.submission_id') }}
