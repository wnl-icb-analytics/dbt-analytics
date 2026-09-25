with contact_measures as (
    select
        referral_id
        , count(*) as n_contacts
        , count_if(is_attended) as n_attended_contacts
        , count_if(is_dna) as n_dna_contacts
        , count_if(is_cancelled) as n_cancelled_contacts
        , count_if(is_attended is null) as n_contacts_with_missing_attendance
        , count_if(is_referral_person_consistent = false) as n_contacts_with_different_referral_person
        , sum(iff(is_attended, clinical_contact_duration_minutes, null)) as attended_contact_minutes
        , min(care_contact_date) as first_contact_date
        , max(care_contact_date) as latest_contact_date
        , min(iff(is_attended, care_contact_date, null)) as first_attended_contact_date
        , max(iff(is_attended, care_contact_date, null)) as latest_attended_contact_date
    from {{ ref('fct_csds_care_contact') }}
    group by referral_id
)

-- Activities in the same submission as each referral's latest contact rows.
, activity_measures as (
    select
        c.referral_id
        , count(*) as n_care_activities
        , count(distinct a.activity_type_code) as n_care_activity_types
    from {{ ref('fct_csds_care_contact') }} as c
    inner join {{ ref('fct_csds_care_activity') }} as a
        on c.source_row_id = a.contact_source_row_id
    group by c.referral_id
)

, team_measures as (
    select
        referral_id
        , count(*) as n_service_team_relationships
        , count(distinct service_type_code) as n_service_team_types
    from {{ ref('fct_csds_referral_service') }}
    group by referral_id
)

, rtt_measures as (
    select
        referral_source_record_id
        , count_if(is_latest_clock_record) as n_rtt_clocks
    from {{ ref('fct_csds_referral_to_treatment_period') }}
    group by referral_source_record_id
)

, provider_latest as (
    select provider_organisation_code, reporting_period_end_date as provider_latest_reporting_period_end_date
    from {{ ref('dq_csds_provider_submission') }}
    where is_latest_provider_period
)

select
    r.*
    , d.as_of_date
    , pl.provider_latest_reporting_period_end_date
    -- A referral absent from its provider's latest submission is treated as closed.
    , r.reporting_period_end_date < pl.provider_latest_reporting_period_end_date as is_no_longer_submitted
    , coalesce(p.is_recorded_open_at_period_end and not is_no_longer_submitted, false) as is_recorded_open
    , iff(is_no_longer_submitted, 'no_longer_submitted', p.recorded_access_state) as latest_recorded_access_state
    , p.has_no_team_recorded
    , p.service_or_team_type_code
    , p.service_or_team_type_name
    , coalesce(c.n_contacts, 0) as n_contacts
    , coalesce(c.n_attended_contacts, 0) as n_attended_contacts
    , coalesce(c.n_dna_contacts, 0) as n_dna_contacts
    , coalesce(c.n_cancelled_contacts, 0) as n_cancelled_contacts
    , coalesce(c.n_contacts_with_missing_attendance, 0) as n_contacts_with_missing_attendance
    , coalesce(c.n_contacts_with_different_referral_person, 0) as n_contacts_with_different_referral_person
    , c.attended_contact_minutes
    , c.first_contact_date
    , c.latest_contact_date
    , c.first_attended_contact_date
    , c.latest_attended_contact_date
    , iff(c.first_attended_contact_date < r.referral_received_date, null,
        datediff(day, r.referral_received_date, c.first_attended_contact_date))
        as days_to_first_attended_contact
    , coalesce(c.first_attended_contact_date < r.referral_received_date, false)
        as has_pre_referral_attended_contact
    , coalesce(a.n_care_activities, 0) as n_care_activities
    , coalesce(a.n_care_activity_types, 0) as n_care_activity_types
    , coalesce(t.n_service_team_relationships, 0) as n_service_team_relationships
    , coalesce(t.n_service_team_types, 0) as n_service_team_types
    , coalesce(rtt.n_rtt_clocks, 0) as n_rtt_clocks
from {{ ref('fct_csds_referral') }} as r
cross join {{ ref('int_csds_reporting_date') }} as d
left join provider_latest as pl on r.provider_organisation_code = pl.provider_organisation_code
left join {{ ref('fct_csds_referral_period') }} as p
    on r.source_row_id = p.source_row_id
    and r.submission_id = p.submission_id
left join contact_measures as c on r.source_record_id = c.referral_id
left join activity_measures as a on r.source_record_id = a.referral_id
left join team_measures as t on r.source_record_id = t.referral_id
left join rtt_measures as rtt on r.source_record_id = rtt.referral_source_record_id
