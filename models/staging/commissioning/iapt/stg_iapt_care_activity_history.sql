{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    post_hook="{{ iapt_remove_withdrawn_submissions() }}"
) }}

select
    s.submission_id
    , a.unique_id_ids202::varchar as source_row_id
    , upper(nullif(trim(a.org_id_provider), '')) as provider_organisation_code
    , nullif(trim(a.person_id), '') as person_id
    , nullif(trim(a.unique_service_request_id), '') as referral_id
    , nullif(trim(a.unique_care_contact_id), '') as care_contact_id
    , nullif(trim(a.unique_care_activity_id), '') as care_activity_id
    , nullif(trim(a.care_act_id), '') as care_act_id
    , nullif(trim(a.care_contact_id), '') as local_care_contact_id
    , nullif(trim(a.service_request_id), '') as service_request_id
    , nullif(trim(a.pathway_id), '') as pathway_id
    , nullif(trim(a.care_pers_local_id), '') as care_pers_local_id
    , nullif(trim(a.unique_care_personnel_id_local), '') as unique_care_personnel_id_local
    , a.clin_contact_dur_of_care_act
    -- SNOMED expressions keep their case and syntax.
    , nullif(trim(a.code_proc_and_proc_status), '') as code_proc_and_proc_status
    , nullif(trim(a.find_scheme_in_use), '') as find_scheme_in_use
    , nullif(trim(a.code_find), '') as code_find
    , nullif(trim(a.validated_finding_code), '') as validated_finding_code
    , nullif(trim(a.code_obs), '') as code_obs
    , nullif(trim(a.obs_value), '') as obs_value
    , nullif(trim(a.unit_measure), '') as unit_measure
    , a.dmic_activity_date::date as dmic_activity_date
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.unique_month_id
    , a.effective_from as source_file_received_at
    , a.dmic_date_added as source_loaded_at
    , s.dataset_version
    , s.file_type
from {{ ref('raw_iapt_ids202careactivity') }} as a
inner join {{ ref('stg_iapt_activesubmission') }} as s
    on a.unique_submission_id = s.submission_id
where s.reporting_period_end_date is not null
    and s.provider_organisation_code is not null
    and {{ iapt_unseen_submission('s.submission_id') }}
