{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key='submission_id', on_schema_change='fail',
    post_hook="{{ iapt_remove_withdrawn_submissions() }}"
) }}

-- Onward referrals have no identifier. ETOS says duplicates of this natural key are rejected, but accepted
-- submissions still contain a few, so every source row is kept and the fact selects one per key.
with onward_referrals as (
    select
        s.submission_id
        , o.unique_id_ids105::varchar as source_row_id
        , upper(nullif(trim(o.org_id_provider), '')) as provider_organisation_code
        , nullif(trim(o.person_id), '') as person_id
        , nullif(trim(o.unique_service_request_id), '') as referral_id
        , nullif(trim(o.service_request_id), '') as service_request_id
        , nullif(trim(o.pathway_id), '') as pathway_id
        , o.onward_refer_date::date as onward_refer_date
        , o.onward_refer_time::time as onward_refer_time
        , nullif(trim(o.onward_refer_reason), '') as onward_refer_reason
        , upper(nullif(trim(o.org_id_receiving), '')) as org_id_receiving
        , s.reporting_period_start_date
        , s.reporting_period_end_date
        , s.unique_month_id
        , o.effective_from as source_file_received_at
        , o.dmic_date_added as source_loaded_at
        , s.dataset_version
        , s.file_type
    from {{ ref('raw_iapt_ids105onwardreferral') }} as o
    inner join {{ ref('stg_iapt_activesubmission') }} as s
        on o.unique_submission_id = s.submission_id
    where s.reporting_period_end_date is not null
        and s.provider_organisation_code is not null
        and {{ iapt_unseen_submission('s.submission_id') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'referral_id', 'onward_refer_date', 'onward_refer_time', 'onward_refer_reason', 'org_id_receiving'
    ]) }} as onward_referral_id
    , *
from onward_referrals
