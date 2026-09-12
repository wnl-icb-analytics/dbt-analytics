{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    pre_hook="{{ source_history_warehouse() }}",
    post_hook=["{{ iapt_remove_withdrawn_submissions() }}", "{{ source_history_warehouse(restore=true) }}"]
) }}

select
    s.submission_id
    , r.unique_id_ids101::varchar as source_row_id
    , upper(nullif(trim(r.org_id_provider), '')) as provider_organisation_code
    , nullif(trim(r.person_id), '') as person_id
    , nullif(trim(r.unique_service_request_id), '') as referral_id
    , nullif(trim(r.service_request_id), '') as service_request_id
    , nullif(trim(r.pathway_id), '') as pathway_id
    , r.use_pathway_flag
    , upper(nullif(trim(r.org_id_comm), '')) as org_id_comm
    , r.referral_request_received_date::date as referral_request_received_date
    -- v2.1 submits the IAPT source list; v2.0 submitted the mental health list.
    , nullif(trim(r.source_of_referral_iapt), '') as source_of_referral_iapt
    , nullif(trim(r.source_of_referral_mh), '') as source_of_referral_mh
    , nullif(trim(r.onset_date), '') as onset_date
    , nullif(trim(r.prev_diag_cond_ind), '') as prev_diag_cond_ind
    , nullif(trim(r.end_code), '') as end_code
    , r.serv_disch_date::date as serv_disch_date
    , r.age_referral_request_received_date
    , r.age_service_discharge_date
    , r.assessment_first_date::date as assessment_first_date
    , r.assessment_last_date::date as assessment_last_date
    , r.therapy_session_first_date::date as therapy_session_first_date
    , r.therapy_session_second_date::date as therapy_session_second_date
    , r.therapy_session_last_date::date as therapy_session_last_date
    , r.care_contact_count
    , r.treatment_care_contact_count
    , r.completed_treatment_flag
    , nullif(trim(r.phq9_first_score), '') as phq9_first_score
    , nullif(trim(r.phq9_last_score), '') as phq9_last_score
    , nullif(trim(r.gad_first_score), '') as gad_first_score
    , nullif(trim(r.gad_last_score), '') as gad_last_score
    , nullif(trim(r.adsm), '') as adsm
    , r.adsm_first_score
    , r.adsm_last_score
    , r.caseness_flag
    , r.not_caseness_flag
    , r.recovery_flag
    , r.reliable_improvement_flag
    , r.no_change_flag
    , r.reliable_deterioration_flag
    , nullif(trim(r.presenting_complaint_higher_category), '') as presenting_complaint_higher_category
    , nullif(trim(r.presenting_complaint_lower_category), '') as presenting_complaint_lower_category
    -- Pre-v2.1.4 names of the category derivations, kept for comparison.
    , nullif(trim(r.primary_presenting_complaint), '') as primary_presenting_complaint
    , nullif(trim(r.secondary_presenting_complaint), '') as secondary_presenting_complaint
    , upper(nullif(trim(r.dm_icb_commissioner), '')) as dm_icb_commissioner
    , upper(nullif(trim(r.dm_sub_icb_commissioner), '')) as dm_sub_icb_commissioner
    , nullif(trim(r.dm_commissioner_derivation_reason), '') as dm_commissioner_derivation_reason
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.unique_month_id
    , r.effective_from as source_file_received_at
    , r.dmic_date_added as source_loaded_at
    , s.dataset_version
    , s.file_type
from {{ ref('raw_iapt_ids101referral') }} as r
inner join {{ ref('stg_iapt_activesubmission') }} as s
    on r.unique_submission_id = s.submission_id
where s.reporting_period_end_date is not null
    and s.provider_organisation_code is not null
    and {{ iapt_unseen_submission('s.submission_id') }}
