{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    post_hook="{{ iapt_remove_withdrawn_submissions() }}"
) }}

select
    s.submission_id
    , c.unique_id_ids201::varchar as source_row_id
    , upper(nullif(trim(c.org_id_provider), '')) as provider_organisation_code
    , nullif(trim(c.person_id), '') as person_id
    , nullif(trim(c.unique_service_request_id), '') as referral_id
    , nullif(trim(c.unique_care_contact_id), '') as care_contact_id
    , nullif(trim(c.care_contact_id), '') as local_care_contact_id
    , nullif(trim(c.service_request_id), '') as service_request_id
    , nullif(trim(c.pathway_id), '') as pathway_id
    , c.care_cont_date::date as care_cont_date
    , c.care_cont_time::time as care_cont_time
    , upper(nullif(trim(c.org_id_comm), '')) as org_id_comm
    , nullif(trim(c.planned_care_cont_indicator), '') as planned_care_cont_indicator
    , nullif(trim(c.attend_or_dna_code), '') as attend_or_dna_code
    , nullif(trim(c.cancellation), '') as cancellation
    , c.clin_cont_dur_of_care_cont
    , nullif(trim(c.iaptltc_service_ind), '') as iaptltc_service_ind
    , nullif(trim(c.app_type), '') as app_type
    -- v2.1 submits consultation mechanism; v2.0 submitted consultation medium used.
    , nullif(trim(c.cons_mechanism), '') as cons_mechanism
    , nullif(trim(c.cons_medium_used), '') as cons_medium_used
    , nullif(trim(c.care_cont_patient_ther_mode), '') as care_cont_patient_ther_mode
    , nullif(trim(c.num_group_ther_participants), '') as num_group_ther_participants
    , nullif(trim(c.num_group_ther_facilitators), '') as num_group_ther_facilitators
    , nullif(trim(c.psych_med), '') as psych_med
    , nullif(trim(c.act_loc_type_code), '') as act_loc_type_code
    , upper(nullif(trim(c.site_id_of_treat), '')) as site_id_of_treat
    , nullif(trim(c.language_code_treat), '') as language_code_treat
    , nullif(trim(c.interpreter_present_ind), '') as interpreter_present_ind
    , c.age_care_contact_date
    , upper(nullif(trim(c.dm_icb_commissioner), '')) as dm_icb_commissioner
    , upper(nullif(trim(c.dm_sub_icb_commissioner), '')) as dm_sub_icb_commissioner
    , nullif(trim(c.dm_commissioner_derivation_reason), '') as dm_commissioner_derivation_reason
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.unique_month_id
    , c.effective_from as source_file_received_at
    , c.dmic_date_added as source_loaded_at
    , s.dataset_version
    , s.file_type
from {{ ref('raw_iapt_ids201carecontact') }} as c
inner join {{ ref('stg_iapt_activesubmission') }} as s
    on c.unique_submission_id = s.submission_id
where s.reporting_period_end_date is not null
    and s.provider_organisation_code is not null
    and {{ iapt_unseen_submission('s.submission_id') }}
