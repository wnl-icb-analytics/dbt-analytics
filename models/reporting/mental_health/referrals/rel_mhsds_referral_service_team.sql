with primary_teams as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "r.uniq_serv_req_id",
            "'primary'",
            "coalesce(r.uniq_care_prof_team_local_id, r.care_prof_team_local_id)"
        ]) }} as referral_service_team_id
        , r.uniq_serv_req_id
        , r.mhs101_uniq_id as source_record_id
        , 'MHS101' as source_record_type
        , 'primary' as service_or_team_relationship_type
        , r.care_prof_team_local_id as service_or_team_local_id
        , coalesce(r.uniq_care_prof_team_local_id, r.care_prof_team_local_id)
            as service_or_team_id
        , coalesce(r.serv_team_type, td.serv_team_type_mh) as service_or_team_type_code
        , coalesce(r.service_type_name, td.service_type_name) as source_service_or_team_type_name
        , coalesce(r.serv_team_int_age_group, td.serv_team_int_age_group)
            as service_or_team_intended_age_group_code
        , r.org_id_prov as provider_organisation_code
        , r.record_start_date
        , r.record_end_date
        , r.refer_rejection_date
        , r.refer_rejection_time
        , r.refer_reject_reason as rejection_reason_code
        , r.serv_disch_date as closure_date
        , r.serv_disch_time as closure_time
        , r.refer_clos_reason as closure_reason_code
        , r.uniq_submission_id
        , r.reporting_period_end_date
        , r.dmic_dataset as mhsds_version
        , r.effective_from as source_file_received_at
        , r.dmic_date_added as source_loaded_at
    from {{ ref('stg_mhsds_referral') }} as r
    left join {{ ref('stg_mhsds_service_or_team_details') }} as td
        on r.care_prof_team_local_id = td.care_prof_team_local_id
        and r.uniq_submission_id = td.uniq_submission_id
    where r.dmic_dataset = 'V6'
        and coalesce(
            r.uniq_care_prof_team_local_id
            , r.care_prof_team_local_id
        ) is not null
)

, other_teams as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "s.uniq_serv_req_id",
            "iff(s.dmic_dataset = 'V6', 'additional', 'referred_to')",
            "s.service_or_team_id"
        ]) }} as referral_service_team_id
        , s.uniq_serv_req_id
        , s.mhs102_uniq_id as source_record_id
        , 'MHS102' as source_record_type
        , iff(s.dmic_dataset = 'V6', 'additional', 'referred_to')
            as service_or_team_relationship_type
        , coalesce(s.other_care_prof_team_local_id, s.care_prof_team_local_id)
            as service_or_team_local_id
        , s.service_or_team_id
        , coalesce(s.serv_team_type_ref_to_mh, td.serv_team_type_mh)
            as service_or_team_type_code
        , coalesce(s.service_type_name, td.service_type_name)
            as source_service_or_team_type_name
        , coalesce(s.serv_team_int_age_group, td.serv_team_int_age_group)
            as service_or_team_intended_age_group_code
        , s.org_id_prov as provider_organisation_code
        , s.record_start_date
        , s.record_end_date
        , s.refer_rejection_date
        , s.refer_rejection_time
        , s.refer_reject_reason as rejection_reason_code
        , s.refer_closure_date as closure_date
        , s.refer_closure_time as closure_time
        , s.refer_clos_reason as closure_reason_code
        , s.uniq_submission_id
        , s.reporting_period_end_date
        , s.dmic_dataset as mhsds_version
        , s.effective_from as source_file_received_at
        , s.dmic_date_added as source_loaded_at
    from {{ ref('stg_mhsds_other_service_or_team_type') }} as s
    left join {{ ref('stg_mhsds_service_or_team_details') }} as td
        on coalesce(s.other_care_prof_team_local_id, s.care_prof_team_local_id)
            = td.care_prof_team_local_id
        and s.uniq_submission_id = td.uniq_submission_id
)

, relationships as (
    select * from primary_teams
    union all
    select * from other_teams
)

select
    r.referral_service_team_id
    , r.uniq_serv_req_id as referral_source_record_id
    , r.source_record_id
    , r.source_record_type
    , r.service_or_team_relationship_type
    , r.service_or_team_id
    , r.service_or_team_local_id
    , r.service_or_team_type_code
    , tt.description as service_or_team_type_description
    , r.source_service_or_team_type_name
    , r.service_or_team_intended_age_group_code
    , intended_age_group.description as service_or_team_intended_age_group_description
    , r.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , r.record_start_date
    , r.record_end_date
    , r.refer_rejection_date as referral_rejection_date
    , r.refer_rejection_time as referral_rejection_time
    , case
        when r.refer_rejection_date is null then null
        when r.refer_rejection_time is null
            then r.refer_rejection_date::timestamp_ntz
        else timestamp_ntz_from_parts(
            r.refer_rejection_date
            , r.refer_rejection_time
        )
    end as referral_rejected_at
    , case
        when r.refer_rejection_date is null then null
        when r.refer_rejection_time is null then 'date'
        else 'timestamp'
    end as referral_rejected_time_precision
    , r.rejection_reason_code
    , rejection_reason.description as rejection_reason_description
    , r.closure_date as referral_closure_date
    , r.closure_time as referral_closure_time
    , case
        when r.closure_date is null then null
        when r.closure_time is null then r.closure_date::timestamp_ntz
        else timestamp_ntz_from_parts(r.closure_date, r.closure_time)
    end as referral_closed_at
    , case
        when r.closure_date is null then null
        when r.closure_time is null then 'date'
        else 'timestamp'
    end as referral_closed_time_precision
    , r.closure_reason_code
    , closure_reason.description as closure_reason_description
    , case
        when r.refer_rejection_date is not null then 'rejected'
        when r.closure_date is not null then 'closed'
        else 'open'
    end as service_or_team_relationship_status
    , r.uniq_submission_id
    , r.reporting_period_end_date
    , r.mhsds_version
    , r.source_file_received_at
    , r.source_loaded_at
from relationships as r
left join {{ ref('mhsds_service_or_team_type') }} as tt
    on r.service_or_team_type_code = tt.code
left join {{ ref('mhsds_service_or_team_intended_age_group') }} as intended_age_group
    on upper(trim(r.service_or_team_intended_age_group_code)) = intended_age_group.code
left join {{ ref('mhsds_referral_code_lookup') }} as rejection_reason
    on upper(trim(r.rejection_reason_code)) = rejection_reason.code
    and rejection_reason.code_set_name = 'rejection_reason'
left join {{ ref('mhsds_referral_code_lookup') }} as closure_reason
    on upper(trim(r.closure_reason_code)) = closure_reason.code
    and closure_reason.code_set_name = 'closure_reason'
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(r.provider_organisation_code) = upper(provider.organisation_code)
