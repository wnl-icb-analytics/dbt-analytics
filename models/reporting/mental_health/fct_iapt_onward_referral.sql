with onward_referrals as (
    select
        *
        -- A submission can repeat the same onward referral, so count periods rather than rows.
        , count(distinct reporting_period_end_date) over (partition by onward_referral_id) as reported_period_count
    from {{ ref('stg_iapt_onward_referral_history') }}
    qualify row_number() over (
        partition by onward_referral_id
        order by
            unique_month_id desc nulls last
            , source_file_received_at desc nulls last
            , try_to_number(submission_id) desc nulls last
            , try_to_number(source_row_id) desc nulls last
            , submission_id desc
            , source_row_id desc
    ) = 1
)

select
    o.onward_referral_id as source_record_id
    , 'IAPT' as source_dataset
    , o.referral_id
    , o.service_request_id as local_referral_id
    , o.pathway_id
    , o.source_row_id
    , o.person_id
    , b.sk_patient_id
    , o.onward_refer_date as onward_referral_date
    , o.onward_refer_time as onward_referral_time
    , iff(
        o.onward_refer_date is null
        , null
        , timestamp_ntz_from_parts(o.onward_refer_date, coalesce(o.onward_refer_time, '00:00:00'::time))
    ) as onward_referral_at
    , case
        when o.onward_refer_date is null then 'unknown'
        when o.onward_refer_time is null then 'date'
        else 'timestamp'
    end as onward_referral_time_precision
    , o.onward_refer_reason as onward_referral_reason_code
    , reason.description as onward_referral_reason_name
    , o.org_id_receiving as receiving_organisation_code
    , receiving.organisation_name as receiving_organisation_name
    , r.source_record_id is not null as is_referral_linked
    , case
        when r.source_record_id is null or o.person_id is null or r.person_id is null then null
        else o.person_id = r.person_id
    end as is_referral_person_consistent
    , o.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , o.reported_period_count
    , o.submission_id
    , o.unique_month_id
    , o.reporting_period_start_date
    , o.reporting_period_end_date
    , o.source_file_received_at
    , o.source_loaded_at
    , o.file_type
    , o.dataset_version
from onward_referrals as o
left join {{ ref('stg_iapt_bridging') }} as b
    on o.person_id = b.person_id
left join {{ ref('fct_iapt_referral') }} as r
    on o.referral_id = r.referral_id
left join {{ ref('iapt_code_lookup') }} as reason
    on reason.code_set_name = 'onward_referral_reason'
    and upper(o.onward_refer_reason) = reason.code
left join {{ ref('organisation') }} as receiving
    on o.org_id_receiving = receiving.organisation_code
left join {{ ref('organisation') }} as provider
    on o.provider_organisation_code = provider.organisation_code
