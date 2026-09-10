with latest as (
    select * from {{ ref('stg_csds_service_type_history') }}
    qualify row_number() over (
        partition by unique_service_request_identifier, care_professional_team_local_identifier
        order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
            unique_submission_id desc, cyp102_unique_id desc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['t.unique_service_request_identifier', 't.care_professional_team_local_identifier']) }} as source_record_id
    , 'CSDS' as source_dataset
    , t.cyp102_unique_id as source_row_id
    , t.unique_service_request_identifier as referral_id
    , t.service_request_identifier as local_referral_id
    , t.care_professional_team_local_identifier as local_team_id
    , t.unique_care_professional_team_local_identifier as team_id
    , t.person_id
    , b.sk_patient_id
    , t.service_or_team_type_referred_to_community_care as service_type_code
    , team.description as service_type_name
    , t.referral_closure_date::date as referral_closure_date
    , t.referral_rejection_date::date as referral_rejection_date
    , t.referral_closure_reason as referral_closure_reason_code
    , closure.description as referral_closure_reason_name
    , t.referral_rejection_reason as referral_rejection_reason_code
    , rejection.description as referral_rejection_reason_name
    , r.cyp101_unique_id as referral_source_row_id
    , r.cyp101_unique_id is not null as is_submitted_referral_linked
    , case when r.cyp101_unique_id is null or t.person_id is null or r.person_id is null then null
        else t.person_id = r.person_id end as is_submitted_referral_person_consistent
    , t.organisation_code_provider as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , t.unique_submission_id as submission_id
    , t.reporting_period_start_date::date as reporting_period_start_date
    , t.reporting_period_end_date::date as reporting_period_end_date
    , t.effective_from as source_file_received_at
    , t.csds_version
    , t.file_type
    , latest_parent.source_record_id is not null as is_referral_linked
    , case when latest_parent.source_record_id is null or t.person_id is null or latest_parent.person_id is null then null
        else t.person_id = latest_parent.person_id end as is_referral_person_consistent
    , case when latest_parent.source_record_id is null or r.cyp101_unique_id is null then null
        else r.cyp101_unique_id = latest_parent.source_row_id end as is_referral_occurrence_consistent
from latest as t
left join {{ ref('stg_csds_referral_history') }} as r
    on t.unique_submission_id = r.unique_submission_id
    and t.unique_service_request_identifier = r.unique_service_request_identifier
left join {{ ref('stg_csds_bridging') }} as b on t.person_id = b.person_id
left join {{ ref('csds_service_or_team_type') }} as team
    on trim(t.service_or_team_type_referred_to_community_care) = team.code
left join {{ ref('csds_activity_code_lookup') }} as closure
    on closure.code_set_name = 'referral_closure_reason' and upper(trim(t.referral_closure_reason)) = closure.code
left join {{ ref('csds_activity_code_lookup') }} as rejection
    on rejection.code_set_name = 'referral_rejection_reason' and upper(trim(t.referral_rejection_reason)) = rejection.code
left join {{ ref('fct_csds_referral') }} as latest_parent
    on latest_parent.source_record_id = t.unique_service_request_identifier
left join {{ ref('organisation') }} as provider
    on upper(trim(t.organisation_code_provider)) = provider.organisation_code
