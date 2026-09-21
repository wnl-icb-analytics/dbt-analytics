with contact_evidence as (
    select
        uniq_serv_req_id
        , greatest(reporting_period_end_date, care_cont_date) as evidence_usable_from_date
        , min(care_cont_date) as first_attended_date_in_evidence
        , max(care_cont_date) as last_attended_date_in_evidence
        , min(person_id) as minimum_attendance_person_id
        , max(person_id) as maximum_attendance_person_id
    from {{ ref('stg_mhsds_carecontact') }}
    where lpad(attend_status, 2, '0') in ('05', '06') and care_cont_date is not null
    group by 1, 2
)
, attendance_history as (
    select
        uniq_serv_req_id
        , evidence_usable_from_date
        , min(first_attended_date_in_evidence) over (
            partition by uniq_serv_req_id order by evidence_usable_from_date
            rows between unbounded preceding and current row
        ) as first_recorded_attended_contact_date
        , max(last_attended_date_in_evidence) over (
            partition by uniq_serv_req_id order by evidence_usable_from_date
            rows between unbounded preceding and current row
        ) as last_recorded_attended_contact_date
        , min(minimum_attendance_person_id) over (
            partition by uniq_serv_req_id order by evidence_usable_from_date
            rows between unbounded preceding and current row
        ) as minimum_attendance_person_id
        , max(maximum_attendance_person_id) over (
            partition by uniq_serv_req_id order by evidence_usable_from_date
            rows between unbounded preceding and current row
        ) as maximum_attendance_person_id
    from contact_evidence
)
select
    {{ dbt_utils.generate_surrogate_key(['r.uniq_submission_id', 'r.uniq_serv_req_id']) }}
        as referral_period_id
    , r.uniq_serv_req_id as referral_source_record_id
    , r.mhs101_uniq_id as source_row_id
    , r.person_id
    , b.sk_patient_id
    , {{ dbt_utils.generate_surrogate_key(['r.person_id', 'r.org_id_prov', 'r.reporting_period_end_date']) }}
        as person_provider_period_id
    , r.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , r.dm_icb_commissioner as source_derived_icb_commissioner_code
    , commissioner.organisation_name as source_derived_icb_commissioner_name
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.referral_request_received_date as referral_received_date
    , iff(r.refer_rejection_date >= '1901-01-01'::date, r.refer_rejection_date, null)
        as referral_rejection_date
    , iff(r.serv_disch_date >= '1901-01-01'::date, r.serv_disch_date, null) as referral_discharge_date
    , r.prim_reason_referral_mh as primary_reason_for_referral_code
    , reason.description as primary_reason_for_referral_description
    , r.clin_resp_priority_type as clinical_response_priority_code
    , priority.description as clinical_response_priority_description
    , r.source_of_referral_mh as referral_source_code
    , referral_source.description as referral_source_description
    , r.care_prof_team_local_id as primary_service_or_team_local_id
    , coalesce(r.serv_team_type, team.serv_team_type_mh) as primary_service_or_team_type_code
    , team_type.description as primary_service_or_team_type_description
    , a.first_recorded_attended_contact_date
    , a.last_recorded_attended_contact_date
    -- National person identifiers can change while the source referral key stays stable.
    , coalesce(r.person_id <> a.minimum_attendance_person_id
        or r.person_id <> a.maximum_attendance_person_id, false) as has_attendance_person_identifier_difference
    , a.evidence_usable_from_date as attendance_evidence_available_by_date
    , r.referral_request_received_date <= r.reporting_period_end_date
        and (referral_rejection_date is null or referral_rejection_date > r.reporting_period_end_date)
        and (referral_discharge_date is null or referral_discharge_date > r.reporting_period_end_date)
        as is_recorded_open_at_period_end
    , case
        when r.referral_request_received_date is null then 'referral_date_missing'
        when r.referral_request_received_date > r.reporting_period_end_date then 'not_started'
        when referral_rejection_date <= r.reporting_period_end_date then 'rejected'
        when referral_discharge_date <= r.reporting_period_end_date then 'discharged'
        when a.first_recorded_attended_contact_date is not null then 'attended_contact_recorded'
        else 'no_attended_contact_recorded'
    end as recorded_access_state
    , is_recorded_open_at_period_end and a.first_recorded_attended_contact_date is null
        as is_open_without_recorded_attendance
    , iff(is_open_without_recorded_attendance,
        datediff(day, r.referral_request_received_date, r.reporting_period_end_date), null)
        as days_open_without_recorded_attendance
    , iff(a.first_recorded_attended_contact_date >= r.referral_request_received_date,
        datediff(day, r.referral_request_received_date, a.first_recorded_attended_contact_date), null)
        as observed_days_to_first_attendance
    , coalesce(a.first_recorded_attended_contact_date < r.referral_request_received_date, false)
        as has_pre_referral_attendance
    , r.uniq_submission_id as submission_id
    , r.effective_from as source_file_received_at
from {{ ref('stg_mhsds_referral_history') }} as r
asof join attendance_history as a
    match_condition(r.reporting_period_end_date >= a.evidence_usable_from_date)
    on r.uniq_serv_req_id = a.uniq_serv_req_id
left join {{ ref('stg_mhsds_bridging') }} as b on r.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(r.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner
    on upper(r.dm_icb_commissioner) = upper(commissioner.organisation_code)
left join {{ ref('mhsds_referral_code_lookup') }} as reason
    on r.prim_reason_referral_mh = reason.code and reason.code_set_name = 'primary_reason_for_referral'
left join {{ ref('mhsds_referral_code_lookup') }} as priority
    on r.clin_resp_priority_type = priority.code and priority.code_set_name = 'clinical_response_priority'
left join {{ ref('mhsds_source_of_referral') }} as referral_source
    on r.source_of_referral_mh = referral_source.code
left join {{ ref('stg_mhsds_service_or_team_details') }} as team
    on r.uniq_submission_id = team.uniq_submission_id and r.care_prof_team_local_id = team.care_prof_team_local_id
left join {{ ref('mhsds_service_or_team_type') }} as team_type
    on coalesce(r.serv_team_type, team.serv_team_type_mh) = team_type.code
