-- Attended contacts count as evidence only once both the contact date and the
-- reporting period that carried them have passed.
with contact_evidence as (
    select
        unique_service_request_identifier
        , greatest(reporting_period_end_date::date, care_contact_date::date) as evidence_usable_from_date
        , min(care_contact_date::date) as first_attended_date_in_evidence
        , max(care_contact_date::date) as last_attended_date_in_evidence
    from {{ ref('stg_csds_care_contact_history') }}
    where {{ csds_attendance_code('attended_or_did_not_attend_code', 'attendance_status') }} in ('5', '6')
        and care_contact_date is not null
    group by 1, 2
)

, attendance_history as (
    select
        unique_service_request_identifier
        , evidence_usable_from_date
        , min(first_attended_date_in_evidence) over (
            partition by unique_service_request_identifier order by evidence_usable_from_date
            rows between unbounded preceding and current row
        ) as first_recorded_attended_contact_date
        , max(last_attended_date_in_evidence) over (
            partition by unique_service_request_identifier order by evidence_usable_from_date
            rows between unbounded preceding and current row
        ) as last_recorded_attended_contact_date
    from contact_evidence
)

-- Team relationships in the same submission. Closure and rejection belong to a
-- team; the referral stays open while any team in the submission is open. Team
-- sections can be missing or partial, so absent teams are not treated as ended.
, submission_teams as (
    select
        unique_submission_id
        , unique_service_request_identifier
        , count(*) as n_service_teams
        , count_if(
            coalesce(referral_closure_date::date <= reporting_period_end_date::date, false)
            or coalesce(referral_rejection_date::date <= reporting_period_end_date::date, false)
        ) as n_service_teams_ended
        , count(distinct service_or_team_type_referred_to_community_care) as n_service_team_types
        , min(service_or_team_type_referred_to_community_care) as any_service_team_type_code
    from {{ ref('stg_csds_service_type_history') }}
    group by 1, 2
)

, periods as (
    select
        r.*
        , r.referral_request_received_date::date as referral_received_date
        , r.service_discharge_date::date as referral_discharge_date
        , r.reporting_period_start_date::date as period_start_date
        , r.reporting_period_end_date::date as period_end_date
    from {{ ref('stg_csds_referral_history') }} as r
)

select
    {{ dbt_utils.generate_surrogate_key(['r.unique_submission_id', 'r.unique_service_request_identifier']) }}
        as referral_period_id
    , r.unique_service_request_identifier as referral_source_record_id
    , r.cyp101_unique_id as source_row_id
    , r.person_id
    , b.sk_patient_id
    , iff(r.person_id is null, null,
        {{ dbt_utils.generate_surrogate_key(['r.person_id', 'r.organisation_code_provider', 'r.period_end_date']) }})
        as person_provider_period_id
    , r.organisation_code_provider as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , r.dm_icb_commissioner as source_icb_commissioner_code
    , {{ is_wnl_icb_code(['r.dm_icb_commissioner', 'r.dm_sub_icb_commissioner', 'r.organisation_code_code_of_commissioner']) }}
        as is_wnl_commissioner
    , icb.organisation_name as source_icb_commissioner_name
    , r.period_start_date as reporting_period_start_date
    , r.period_end_date as reporting_period_end_date
    , r.referral_received_date
    , r.referral_discharge_date
    , r.ic_age_at_service_referral_received_date as age_at_referral
    , r.primary_reason_for_referral_community_care as primary_reason_for_referral_code
    , reason.description as primary_reason_for_referral_name
    , r.priority_type_code
    , priority.description as priority_type_name
    , r.source_of_referral_for_community as source_of_referral_code
    , source.description as source_of_referral_name
    , coalesce(t.n_service_teams, 0) as n_service_teams
    , coalesce(t.n_service_teams_ended, 0) as n_service_teams_ended
    , iff(t.n_service_team_types = 1, t.any_service_team_type_code, null) as service_or_team_type_code
    , team_type.description as service_or_team_type_name
    , a.first_recorded_attended_contact_date
    , a.last_recorded_attended_contact_date
    , a.evidence_usable_from_date as attendance_evidence_available_by_date
    , r.referral_received_date <= r.period_end_date
        and (r.referral_discharge_date is null or r.referral_discharge_date > r.period_end_date)
        and (t.n_service_teams is null or t.n_service_teams_ended < t.n_service_teams)
        as is_recorded_open_at_period_end
    , t.n_service_teams is null as has_no_team_in_submission
    , case
        when r.referral_received_date is null then 'referral_date_missing'
        when r.referral_received_date > r.period_end_date then 'not_started'
        when r.referral_discharge_date <= r.period_end_date then 'discharged'
        when t.n_service_teams_ended = t.n_service_teams then 'all_teams_ended'
        when a.first_recorded_attended_contact_date is not null then 'attended_contact_recorded'
        else 'no_attended_contact_recorded'
    end as recorded_access_state
    , is_recorded_open_at_period_end and a.first_recorded_attended_contact_date is null
        as is_open_without_recorded_attendance
    , iff(is_open_without_recorded_attendance,
        datediff(day, r.referral_received_date, r.period_end_date), null)
        as days_open_without_recorded_attendance
    , iff(a.first_recorded_attended_contact_date >= r.referral_received_date,
        datediff(day, r.referral_received_date, a.first_recorded_attended_contact_date), null)
        as observed_days_to_first_attendance
    , coalesce(a.first_recorded_attended_contact_date < r.referral_received_date, false)
        as has_pre_referral_attendance
    , r.unique_submission_id as submission_id
    , r.effective_from as source_file_received_at
    , r.csds_version
from periods as r
asof join attendance_history as a
    match_condition(r.period_end_date >= a.evidence_usable_from_date)
    on r.unique_service_request_identifier = a.unique_service_request_identifier
left join submission_teams as t
    on r.unique_submission_id = t.unique_submission_id
    and r.unique_service_request_identifier = t.unique_service_request_identifier
left join {{ ref('stg_csds_bridging') }} as b on r.person_id = b.person_id
left join {{ ref('organisation') }} as provider
    on upper(trim(r.organisation_code_provider)) = provider.organisation_code
left join {{ ref('organisation') }} as icb
    on upper(trim(r.dm_icb_commissioner)) = icb.organisation_code
left join {{ ref('csds_referral_code_lookup') }} as reason
    on reason.code_set_name = 'reason_for_referral' and trim(r.primary_reason_for_referral_community_care) = reason.code
left join {{ ref('csds_referral_code_lookup') }} as priority
    on priority.code_set_name = 'priority_type' and trim(r.priority_type_code) = priority.code
left join {{ ref('csds_source_of_referral') }} as source
    on trim(r.source_of_referral_for_community) = source.code
left join {{ ref('csds_service_or_team_type') }} as team_type
    on trim(iff(t.n_service_team_types = 1, t.any_service_team_type_code, null)) = team_type.code
